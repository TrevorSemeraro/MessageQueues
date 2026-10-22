#pragma once
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <type_traits>

#include <sys/types.h>

namespace MultiQueues {
#ifdef __cpp_lib_hardware_interference_size
inline constexpr std::size_t CACHE_LINE =
	std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t CACHE_LINE = 64;
#endif

enum class ReadStatus : uint8_t { Empty, Ok, Lapped };

struct MessageHeader {
	uint64_t size;
	uint32_t type;

	bool operator==(const MessageHeader &) const = default;
};

namespace detail {
inline constexpr uint64_t PROTOCOL_MAJOR = 2;
inline constexpr uint64_t PROTOCOL_MINOR = 0;
inline constexpr uint64_t MIN_CAPACITY = 4 * CACHE_LINE;
/* Marks a SPAN_HEADER; producers may not use it. */
inline constexpr uint32_t RESERVED_TYPE = std::numeric_limits<uint32_t>::max();

template <uint64_t A>
constexpr uint64_t align_as(const uint64_t n)
{
	static_assert((A & (A - 1)) == 0, "A must be power of two");
	return (n + (A - 1)) & ~(A - 1);
}

struct alignas(CACHE_LINE) Line {
	std::byte b[CACHE_LINE];
};

struct HeaderPrefix {
	uint64_t protocol_major;
	uint64_t protocol_minor;
	uint64_t policy_id;
	uint64_t capacity;
	/* pid of the process holding a single-role producer/consumer, or 0 */
	std::atomic<pid_t> producer_pid{0};
	std::atomic<pid_t> consumer_pid{0};
};

/* Written where a frame would not fit before the end of the buffer. */
inline constexpr MessageHeader SPAN_HEADER{0, RESERVED_TYPE};
/* Payloads start on their own cache line. */
inline constexpr uint64_t PAYLOAD_OFFSET =
	align_as<CACHE_LINE>(sizeof(MessageHeader));

template <class H>
inline constexpr uint64_t QUEUE_HEADER_SIZE = align_as<CACHE_LINE>(sizeof(H));

constexpr uint64_t frame_size(const uint64_t size)
{
	return align_as<CACHE_LINE>(PAYLOAD_OFFSET + size);
}

/*
 * Frames are at most half the ring (less one line), so a frame always fits
 * once the queue is empty, wherever the counters happen to be.
 */
constexpr uint64_t max_payload(const uint64_t capacity)
{
	return capacity / 2 - CACHE_LINE - PAYLOAD_OFFSET;
}

[[noreturn, gnu::cold, gnu::noinline]] inline void throw_invalid_write()
{
	throw std::invalid_argument(
		"MultiQueues: payload exceeds max_payload() or uses the reserved type");
}

inline void check_write(const uint64_t size, const uint32_t type,
						const uint64_t capacity)
{
	if (size > max_payload(capacity) || type == RESERVED_TYPE) [[unlikely]]
		throw_invalid_write();
}

template <class Header>
struct QueueView {
	Header *header{nullptr};
	std::byte *data{nullptr};
	uint64_t capacity{0};

	[[nodiscard]] uint64_t offset_of(const uint64_t counter) const
	{
		return counter & (capacity - 1);
	}
};

template <class H>
concept QueueHeader =
	std::is_standard_layout_v<H> && alignof(H) <= CACHE_LINE && requires(H &h) {
		{ h.prefix } -> std::same_as<HeaderPrefix &>;
	};

struct WriteFnArchetype {
	void *state;
	void operator()(std::span<std::byte>) const
	{
	}
};

struct ReadFnArchetype {
	void *state;
	void operator()(MessageHeader, std::span<std::byte>) const
	{
	}
};
}

template <class P>
concept Producer = requires(P p, std::size_t size, uint32_t type,
							detail::WriteFnArchetype fn) {
	{ p.write(size, type, fn) } -> std::same_as<uint8_t>;
};

template <class C>
concept Consumer = requires(C c, detail::ReadFnArchetype fn) {
	{ c.read(fn) } -> std::same_as<ReadStatus>;
};

template <class P>
concept QueuePolicy = requires {
	typename P::header_type;
	typename P::producer_type;
	typename P::consumer_type;
	{ P::policy_id } -> std::convertible_to<uint64_t>;
	{ P::single_producer } -> std::convertible_to<bool>;
	{ P::single_consumer } -> std::convertible_to<bool>;
} && detail::QueueHeader<typename P::header_type>;
}
