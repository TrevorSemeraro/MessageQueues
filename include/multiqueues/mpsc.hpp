#pragma once
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>

#include "queue.hpp"

namespace MultiQueues {
namespace detail {
/*
 * Set in a frame's size word once the frame is complete. The consumer
 * clears the first word of every line it frees, so a line start reads as
 * uncommitted until a producer commits a frame there.
 */
inline constexpr uint64_t COMMITTED = uint64_t{1} << 63;

/* Both counters are running byte totals; offset_of() maps them to the ring. */
struct MPSCHeader {
	HeaderPrefix prefix;
	/* end of the space the consumer has freed */
	alignas(CACHE_LINE) std::atomic<uint64_t> read_counter{0};
	/* end of the space producers have claimed */
	alignas(CACHE_LINE) std::atomic<uint64_t> write_counter{0};
};

inline std::atomic_ref<uint64_t> size_word(std::byte *frame)
{
	return std::atomic_ref<uint64_t>{*reinterpret_cast<uint64_t *>(frame)};
}

inline void commit(std::byte *frame, const MessageHeader header)
{
	std::memcpy(frame + offsetof(MessageHeader, type), &header.type,
				sizeof(header.type));
	size_word(frame).store(header.size | COMMITTED, std::memory_order_release);
}
}

/* Any number may write concurrently; write() fails rather than overwrite. */
class MPSCProducer {
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
		std::atomic<uint64_t> &tail = queue_.header->write_counter;
		uint64_t pos = tail.load(std::memory_order_relaxed);
		uint64_t pad;

		do {
			const uint64_t offset = queue_.offset_of(pos);
			pad = offset + len > queue_.capacity ? queue_.capacity - offset : 0;
			if (pos + pad + len > cached_read_ + queue_.capacity) {
				cached_read_ =
					queue_.header->read_counter.load(std::memory_order_acquire);
				if (pos + pad + len > cached_read_ + queue_.capacity)
					return 0;
			}
		} while (!tail.compare_exchange_weak(pos, pos + pad + len,
											 std::memory_order_relaxed));

		std::byte *frame = queue_.data + queue_.offset_of(pos);
		if (pad) {
			detail::commit(frame, detail::SPAN_HEADER);
			frame = queue_.data;
		}

		fn(std::span{frame + detail::PAYLOAD_OFFSET, size});
		detail::commit(frame, MessageHeader{size, type});
		return 1;
	}

private:
	using view_type = detail::QueueView<detail::MPSCHeader>;

	friend struct detail::Access;

	explicit MPSCProducer(const view_type queue)
		: queue_{queue}, cached_read_{queue.header->read_counter.load(
							 std::memory_order_acquire)}
	{
	}

	view_type queue_;
	uint64_t cached_read_;
};

/*
 * Frames are delivered in the order they were claimed, so one slow producer
 * holds back the ones behind it until it commits.
 */
class MPSCConsumer {
public:
	template <std::invocable<MessageHeader, std::span<std::byte>> F>
	ReadStatus read(F &&fn)
	{
		const uint64_t offset = queue_.offset_of(read_pos_);
		std::byte *frame = queue_.data + offset;
		const uint64_t word =
			detail::size_word(frame).load(std::memory_order_acquire);
		if (!(word & detail::COMMITTED))
			return ReadStatus::Empty;

		MessageHeader header{word & ~detail::COMMITTED, 0};
		std::memcpy(&header.type, frame + offsetof(MessageHeader, type),
					sizeof(header.type));
		if (header == detail::SPAN_HEADER) {
			release(frame, queue_.capacity - offset);
			return read(std::forward<F>(fn));
		}

		fn(header, std::span{frame + detail::PAYLOAD_OFFSET, header.size});
		release(frame, detail::frame_size(header.size));
		return ReadStatus::Ok;
	}

private:
	using view_type = detail::QueueView<detail::MPSCHeader>;

	friend struct detail::Access;

	MPSCConsumer(const view_type queue, detail::Claim claim)
		: queue_{queue},
		  read_pos_{queue.header->read_counter.load(std::memory_order_relaxed)},
		  claim_{std::move(claim)}
	{
	}

	void release(std::byte *frame, const uint64_t len)
	{
		for (uint64_t off = 0; off < len; off += CACHE_LINE)
			detail::size_word(frame + off).store(0, std::memory_order_relaxed);
		read_pos_ += len;
		queue_.header->read_counter.store(read_pos_, std::memory_order_release);
	}

	view_type queue_;
	uint64_t read_pos_;
	detail::Claim claim_;
};

struct MPSCPolicy {
	using header_type = detail::MPSCHeader;
	using producer_type = MPSCProducer;
	using consumer_type = MPSCConsumer;
	static constexpr uint64_t policy_id = 3;
	static constexpr bool single_producer = false;
	static constexpr bool single_consumer = true;
};
static_assert(Producer<MPSCProducer> && Consumer<MPSCConsumer>);

using MPSC = Queue<MPSCPolicy>;
}
