#pragma once
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <utility>

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include "backing.hpp"
#include "concepts.hpp"

namespace MultiQueues {
namespace detail {
/* Lets the factories reach private constructors and queue internals. */
struct Access {
	template <class Queue>
	static auto &view(Queue &queue)
	{
		return queue.view_;
	}

	template <class T, class... Args>
	static T make(Args &&...args)
	{
		return T{std::forward<Args>(args)...};
	}
};

/* kill() with signal 0 checks that a process exists without signalling it. */
inline bool process_alive(const pid_t pid)
{
	return kill(pid, 0) == 0 || errno == EPERM;
}

/*
 * Exclusive ownership of a producer or consumer role. The owner's pid is
 * kept in the queue header so the claim holds across processes, and a
 * claim left behind by a process that died is taken over.
 */
class Claim {
public:
	explicit Claim(std::atomic<pid_t> &owner) : owner_{&owner}
	{
		const pid_t self = getpid();
		pid_t current = 0;

		while (!owner.compare_exchange_strong(current, self,
											  std::memory_order_acquire)) {
			if (current == self || process_alive(current))
				throw std::logic_error(
					"MultiQueues: this queue allows only one producer/consumer");
		}
	}

	Claim(Claim &&other) noexcept : owner_{std::exchange(other.owner_, nullptr)}
	{
	}

	Claim &operator=(Claim &&other) noexcept
	{
		release();
		owner_ = std::exchange(other.owner_, nullptr);
		return *this;
	}

	~Claim()
	{
		release();
	}

private:
	/* Only clears our own pid, so a forked child cannot drop the parent's. */
	void release()
	{
		if (!owner_)
			return;
		pid_t self = getpid();
		owner_->compare_exchange_strong(self, 0, std::memory_order_release,
										std::memory_order_relaxed);
	}

	std::atomic<pid_t> *owner_;
};
}

/*
 * The memory a queue lives in, created or attached through a backing:
 *
 *	auto queue = SPSC::create(SharedMemory{"/orders"}, 1 << 20);
 *	auto producer = create_producer(queue);
 *
 *	auto queue = SPSC::attach(SharedMemory{"/orders"});
 *	auto consumer = create_consumer(queue);
 *
 * Producers and consumers must not outlive the queue they came from.
 */
template <QueuePolicy Policy>
class Queue {
public:
	using policy_type = Policy;
	using header_type = typename Policy::header_type;
	using view_type = detail::QueueView<header_type>;
	using producer_type = typename Policy::producer_type;
	using consumer_type = typename Policy::consumer_type;

	static constexpr uint64_t header_size =
		detail::QUEUE_HEADER_SIZE<header_type>;

	/* Bytes a queue of `capacity` occupies in its backing. */
	static constexpr std::size_t region_size(const std::size_t capacity)
	{
		return header_size + capacity;
	}

	/* capacity: data bytes, a power of two of at least 4 cache lines. */
	template <class Backing>
	static Queue create(Backing backing, const std::size_t capacity)
	{
		check_capacity(capacity);
		Queue queue{
			detail::Mapping::create(std::move(backing), region_size(capacity))};
		queue.init(capacity);
		return queue;
	}

	/* Joins a queue some process created; its capacity comes from the header.
	 */
	template <class Backing>
	static Queue attach(const Backing &backing)
	{
		Queue queue{detail::Mapping::attach(backing)};
		queue.join();
		return queue;
	}

	Queue(Queue &&) noexcept = default;
	Queue &operator=(Queue &&) noexcept = default;

	[[nodiscard]] std::size_t capacity() const
	{
		return view_.capacity;
	}

private:
	friend struct detail::Access;

	explicit Queue(detail::Mapping mapping) : mapping_{std::move(mapping)}
	{
	}

	static void check_capacity(const std::size_t capacity)
	{
		if (capacity < detail::MIN_CAPACITY || (capacity & (capacity - 1)))
			throw std::invalid_argument(
				"MultiQueues: capacity must be a power of two >= 4 cache lines");
	}

	/* protocol_major is written last: attach treats it as "ready". */
	void init(const std::size_t capacity)
	{
		std::byte *region = mapping_.data();
		std::memset(region, 0, region_size(capacity));

		auto *header = ::new (region) header_type{
			.prefix =
				{
					.protocol_major = 0,
					.protocol_minor = detail::PROTOCOL_MINOR,
					.policy_id = Policy::policy_id,
					.capacity = capacity,
				},
		};
		view_ = view_type{header, region + header_size, capacity};
		std::atomic_ref{header->prefix.protocol_major}.store(
			detail::PROTOCOL_MAJOR, std::memory_order_release);
	}

	void join()
	{
		if (mapping_.size() < header_size)
			throw std::runtime_error(
				"MultiQueues: backing too small for a queue");

		auto *header = reinterpret_cast<header_type *>(mapping_.data());
		detail::HeaderPrefix &prefix = header->prefix;
		if (std::atomic_ref{prefix.protocol_major}.load(
				std::memory_order_acquire) != detail::PROTOCOL_MAJOR ||
			prefix.policy_id != Policy::policy_id)
			throw std::runtime_error(
				"MultiQueues: backing does not hold a ready queue of this protocol/policy");
		if (mapping_.size() < region_size(prefix.capacity))
			throw std::runtime_error(
				"MultiQueues: backing is smaller than the queue it holds");

		view_ =
			view_type{header, mapping_.data() + header_size, prefix.capacity};
	}

	detail::Mapping mapping_;
	view_type view_;
};

/* Throws std::logic_error if a live process holds the producer role. */
template <QueuePolicy Policy>
	requires(Policy::single_producer)
Policy::producer_type create_producer(Queue<Policy> &queue)
{
	auto &view = detail::Access::view(queue);
	return detail::Access::make<typename Policy::producer_type>(
		view, detail::Claim{view.header->prefix.producer_pid});
}

template <QueuePolicy Policy>
	requires(!Policy::single_producer)
Policy::producer_type create_producer(Queue<Policy> &queue)
{
	return detail::Access::make<typename Policy::producer_type>(
		detail::Access::view(queue));
}

/* Throws std::logic_error if a live process holds the consumer role. */
template <QueuePolicy Policy>
	requires(Policy::single_consumer)
Policy::consumer_type create_consumer(Queue<Policy> &queue)
{
	auto &view = detail::Access::view(queue);
	return detail::Access::make<typename Policy::consumer_type>(
		view, detail::Claim{view.header->prefix.consumer_pid});
}

template <QueuePolicy Policy>
	requires(!Policy::single_consumer)
Policy::consumer_type create_consumer(Queue<Policy> &queue)
{
	return detail::Access::make<typename Policy::consumer_type>(
		detail::Access::view(queue));
}
}
