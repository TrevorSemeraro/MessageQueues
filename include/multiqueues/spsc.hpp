#pragma once
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <span>
#include <utility>

#include "queue.hpp"

namespace MultiQueues {
namespace detail {
/* Both counters are offsets into the data region. */
struct SPSCHeader {
	HeaderPrefix prefix;
	alignas(CACHE_LINE) std::atomic<uint64_t> read_counter{0};
	alignas(CACHE_LINE) std::atomic<uint64_t> write_counter{0};
};
}

class SPSCProducer {
public:
	[[nodiscard]] std::size_t max_payload() const
	{
		return detail::max_payload(queue_.capacity);
	}

	template <std::invocable<std::span<std::byte>> F>
	uint8_t write(const std::size_t size, const uint32_t type, F &&fn)
	{
		detail::check_write(size, type, queue_.capacity);
		const uint64_t len = detail::frame_size(size);
		uint64_t pos = write_pos_;

		if (!fits(pos, len)) {
			cached_read_ =
				queue_.header->read_counter.load(std::memory_order_acquire);
			if (!fits(pos, len))
				return 0;
		}

		if (pos + len >= queue_.capacity) {
			::new (queue_.data + pos) MessageHeader{detail::SPAN_HEADER};
			pos = 0;
		}

		::new (queue_.data + pos) MessageHeader{size, type};
		fn(std::span{queue_.data + pos + detail::PAYLOAD_OFFSET, size});

		write_pos_ = pos + len;
		queue_.header->write_counter.store(write_pos_,
										   std::memory_order_release);
		return 1;
	}

private:
	using view_type = detail::QueueView<detail::SPSCHeader>;

	friend struct detail::Access;

	SPSCProducer(const view_type queue, detail::Claim claim)
		: queue_{queue}, write_pos_{queue.header->write_counter.load(
							 std::memory_order_relaxed)},
		  cached_read_{
			  queue.header->read_counter.load(std::memory_order_acquire)},
		  claim_{std::move(claim)}
	{
	}

	/* write_pos_ never lands on cached_read_: equal means empty. */
	[[nodiscard]] bool fits(const uint64_t pos, const uint64_t len) const
	{
		if (pos < cached_read_)
			return pos + len < cached_read_;
		return pos + len < queue_.capacity || len < cached_read_;
	}

	view_type queue_;
	uint64_t write_pos_;
	uint64_t cached_read_;
	detail::Claim claim_;
};

class SPSCConsumer {
public:
	template <std::invocable<MessageHeader, std::span<std::byte>> F>
	ReadStatus read(F &&fn)
	{
		uint64_t pos = read_pos_;

		if (pos == cached_write_) {
			cached_write_ =
				queue_.header->write_counter.load(std::memory_order_acquire);
			if (pos == cached_write_)
				return ReadStatus::Empty;
		}

		MessageHeader header;
		std::memcpy(&header, queue_.data + pos, sizeof(header));
		if (header == detail::SPAN_HEADER) {
			pos = 0;
			std::memcpy(&header, queue_.data, sizeof(header));
		}

		fn(header,
		   std::span{queue_.data + pos + detail::PAYLOAD_OFFSET, header.size});

		read_pos_ = pos + detail::frame_size(header.size);
		queue_.header->read_counter.store(read_pos_, std::memory_order_release);
		return ReadStatus::Ok;
	}

private:
	using view_type = detail::QueueView<detail::SPSCHeader>;

	friend struct detail::Access;

	SPSCConsumer(const view_type queue, detail::Claim claim)
		: queue_{queue},
		  read_pos_{queue.header->read_counter.load(std::memory_order_relaxed)},
		  cached_write_{
			  queue.header->write_counter.load(std::memory_order_acquire)},
		  claim_{std::move(claim)}
	{
	}

	view_type queue_;
	uint64_t read_pos_;
	uint64_t cached_write_;
	detail::Claim claim_;
};

struct SPSCPolicy {
	using header_type = detail::SPSCHeader;
	using producer_type = SPSCProducer;
	using consumer_type = SPSCConsumer;
	static constexpr uint64_t policy_id = 1;
	static constexpr bool single_producer = true;
	static constexpr bool single_consumer = true;
};
static_assert(Producer<SPSCProducer> && Consumer<SPSCConsumer>);

using SPSC = Queue<SPSCPolicy>;
}
