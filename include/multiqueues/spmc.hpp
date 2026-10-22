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
/* Both counters are running byte totals; offset_of() maps them to the ring. */
struct SPMCHeader {
	HeaderPrefix prefix;
	/* end of the last complete frame */
	alignas(CACHE_LINE) std::atomic<uint64_t> published{0};
	/* end of the region the producer may be overwriting */
	alignas(CACHE_LINE) std::atomic<uint64_t> reserved{0};
};
}

/* Never blocks: slow consumers are lapped instead. */
class SPMCProducer {
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

		if (offset_ + len >= queue_.capacity) {
			published_ += queue_.capacity - offset_;
			queue_.header->reserved.store(published_,
										  std::memory_order_release);
			::new (queue_.data + offset_) MessageHeader{detail::SPAN_HEADER};
			offset_ = 0;
		}

		published_ += len;
		if (reserved_ < published_) {
			/* TODO: reserve a block of bytes at a time */
			reserved_ = detail::align_as<CACHE_LINE>(published_);
			queue_.header->reserved.store(reserved_, std::memory_order_release);
		}

		::new (queue_.data + offset_) MessageHeader{size, type};
		fn(std::span{queue_.data + offset_ + detail::PAYLOAD_OFFSET, size});

		offset_ += len;
		queue_.header->published.store(published_, std::memory_order_release);
		return 1;
	}

private:
	using view_type = detail::QueueView<detail::SPMCHeader>;

	friend struct detail::Access;

	SPMCProducer(const view_type queue, detail::Claim claim)
		: queue_{queue},
		  published_{queue.header->published.load(std::memory_order_acquire)},
		  offset_{queue.offset_of(published_)}, reserved_{published_},
		  claim_{std::move(claim)}
	{
	}

	view_type queue_;
	alignas(CACHE_LINE) uint64_t published_;
	uint64_t offset_;	/* where the next frame goes in the data region */
	uint64_t reserved_; /* last value stored to header->reserved */
	detail::Claim claim_;
};

/* Reads only frames published after it was created. */
class SPMCConsumer {
public:
	template <std::invocable<MessageHeader, std::span<std::byte>> F>
	ReadStatus read(F &&fn)
	{
		if (consumed_ == cached_published_) {
			cached_published_ =
				queue_.header->published.load(std::memory_order_acquire);
			if (consumed_ == cached_published_)
				return ReadStatus::Empty;
		}

		MessageHeader header;
		std::memcpy(&header, queue_.data + offset_, sizeof(header));

		uint64_t reserved =
			queue_.header->reserved.load(std::memory_order_acquire);
		if (reserved - consumed_ >= queue_.capacity)
			return ReadStatus::Lapped;

		if (header == detail::SPAN_HEADER) {
			consumed_ += queue_.capacity - offset_;
			offset_ = 0;
			std::memcpy(&header, queue_.data, sizeof(header));
		}

		fn(header, std::span{queue_.data + offset_ + detail::PAYLOAD_OFFSET,
							 header.size});

		const uint64_t len = detail::frame_size(header.size);
		consumed_ += len;
		offset_ += len;

		reserved = queue_.header->reserved.load(std::memory_order_acquire);
		if (reserved - consumed_ >= queue_.capacity)
			return ReadStatus::Lapped;
		return ReadStatus::Ok;
	}

	/* Skips to the newest frame after being lapped. */
	void respawn()
	{
		cached_published_ =
			queue_.header->published.load(std::memory_order_acquire);
		consumed_ = cached_published_;
		offset_ = queue_.offset_of(consumed_);
	}

private:
	using view_type = detail::QueueView<detail::SPMCHeader>;

	friend struct detail::Access;

	explicit SPMCConsumer(const view_type queue) : queue_{queue}
	{
		respawn();
	}

	alignas(CACHE_LINE) uint64_t cached_published_;
	view_type queue_;
	uint64_t offset_;	/* where the next frame is in the data region */
	uint64_t consumed_; /* running total; compared to reserved for laps */
};

struct SPMCPolicy {
	using header_type = detail::SPMCHeader;
	using producer_type = SPMCProducer;
	using consumer_type = SPMCConsumer;
	static constexpr uint64_t policy_id = 2;
	static constexpr bool single_producer = true;
	static constexpr bool single_consumer = false;
};
static_assert(Producer<SPMCProducer> && Consumer<SPMCConsumer>);

using SPMC = Queue<SPMCPolicy>;
}
