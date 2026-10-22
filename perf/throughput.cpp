#include <multiqueues/spmc.hpp>

#include <benchmark/benchmark.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/broadcast/BroadcastReceiver.h>
#include <concurrent/broadcast/BroadcastTransmitter.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <latch>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace throughput_benchmark {
struct ReaderStats {
	uint64_t received = 0;
	uint64_t laps = 0;
	bool out_of_order = false;
};

/*
 * Writer on core 0, reader r on core r + 1. Adjust to your topology (physical
 * cores, one NUMA node).
 */
inline void pin_to_core([[maybe_unused]] const std::size_t core)
{
#ifdef __linux__
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(core, &set);
	pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#endif
}

using namespace MultiQueues;

/*
 * Spins on consumer.read() until the writer is done and the queue is drained.
 * Every message starts with its sequence number, which is how order is checked.
 */
template <Consumer C>
void reader_loop(C &consumer, const std::atomic<bool> &done, std::latch &ready,
				 ReaderStats &out)
{
	ReaderStats stats; /* kept local, published once at the end */
	uint64_t expected = 0;
	uint64_t seq = 0;
	bool last_pass = false;

	ready.count_down();
	for (;;) {
		/*
		 * Nothing is committed inside the callback: if we were lapped
		 * mid-read, seq may be garbage.
		 */
		const ReadStatus status = consumer.read(
			[&seq](MessageHeader, const std::span<const std::byte> msg) {
				std::memcpy(&seq, msg.data(), sizeof(seq));
			});

		if (status == ReadStatus::Empty) {
			/*
			 * done is set after the last write, so an Empty seen
			 * after done means fully drained.
			 */
			if (last_pass)
				break;
			last_pass = done.load(std::memory_order_acquire);
			continue;
		}
		if (status == ReadStatus::Lapped) {
			++stats.laps;
			consumer.respawn();
			continue;
		}
		if (seq < expected) {
			stats.out_of_order = true;
			break;
		}
		expected = seq + 1;
		++stats.received;
	}
	out = stats;
}

/*
 * Publishes as fast as it can, never waiting for readers. One benchmark
 * iteration = one message. publish(payload) must copy the payload into the
 * queue.
 */
template <class Publish>
uint64_t writer_loop(benchmark::State &state, const std::size_t size,
					 Publish &&publish)
{
	std::vector<std::byte> payload(size, std::byte{0xAB});
	uint64_t seq = 0;
	for (auto _ : state) {
		std::memcpy(payload.data(), &seq, sizeof(seq));
		publish(std::span<const std::byte>{payload});
		++seq;
	}
	return seq;
}

/*
 * Shared harness so every queue is measured the same way.
 * create_consumer() is called on this thread before the first write, so every
 * message counts for every reader.
 */
template <class MakeConsumer, class Publish>
void run_throughput(benchmark::State &state, const std::size_t nr_readers,
					const std::size_t size, MakeConsumer &&create_consumer,
					Publish &&publish)
{
	std::atomic<bool> done{false};
	std::latch ready{static_cast<std::ptrdiff_t>(nr_readers)};
	std::vector<ReaderStats> stats(nr_readers);
	std::vector<std::thread> readers;
	readers.reserve(nr_readers);

	for (std::size_t r = 0; r < nr_readers; ++r) {
		readers.emplace_back([consumer = create_consumer(), &done, &ready,
							  &out = stats[r], r]() mutable {
			pin_to_core(r + 1);
			reader_loop(consumer, done, ready, out);
		});
	}
	pin_to_core(0);
	ready.wait(); /* all readers are spinning before the clock starts */

	const uint64_t published = writer_loop(state, size, publish);

	done.store(true, std::memory_order_release);
	for (auto &reader : readers)
		reader.join();

	uint64_t laps = 0;
	uint64_t dropped = 0;
	uint64_t slowest = published;
	for (const ReaderStats &s : stats) {
		if (s.out_of_order) {
			state.SkipWithError("messages arrived out of order");
			return;
		}
		laps += s.laps;
		dropped += published - s.received;
		slowest = std::min(slowest, s.received);
	}

	/*
	 * items_per_second is the published rate. It is the broadcast
	 * throughput only if laps == 0; otherwise slowest_reader is what was
	 * actually delivered to every consumer.
	 */
	state.SetItemsProcessed(static_cast<int64_t>(published));
	state.SetBytesProcessed(static_cast<int64_t>(published * size));
	state.counters["slowest_reader"] = benchmark::Counter(
		static_cast<double>(slowest), benchmark::Counter::kIsRate);
	state.counters["laps"] = benchmark::Counter(static_cast<double>(laps));
	state.counters["dropped"] =
		benchmark::Counter(static_cast<double>(dropped));
}

/* Args: queue capacity, number of readers, payload bytes (>= 8). */
static void BM_SPMC_Throughput(benchmark::State &state)
{
	const auto capacity = static_cast<std::size_t>(state.range(0));
	const auto nr_readers = static_cast<std::size_t>(state.range(1));
	const auto size = static_cast<std::size_t>(state.range(2));

	SPMC queue = SPMC::create(Heap{}, capacity);
	auto producer{create_producer(queue)};

	run_throughput(
		state, nr_readers, size, [&queue] { return create_consumer(queue); },
		[&producer](const std::span<const std::byte> payload) {
			producer.write(
				payload.size(), 1, [payload](const std::span<std::byte> buf) {
					std::memcpy(buf.data(), payload.data(), buf.size());
				});
		});
}

namespace aeron_broadcast {
using aeron::concurrent::AtomicBuffer;
namespace BroadcastBufferDescriptor =
	aeron::concurrent::broadcast::BroadcastBufferDescriptor;
using aeron::concurrent::broadcast::BroadcastReceiver;
using aeron::concurrent::broadcast::BroadcastTransmitter;

/*
 * Adapts BroadcastReceiver to our Consumer interface so reader_loop is shared.
 * Aeron's receiver can't recover from a torn read (CopyBroadcastReceiver
 * throws), so a lap re-creates the receiver, which starts at the latest record
 * -- the same thing SPMCConsumer::respawn does.
 */
class Consumer {
public:
	explicit Consumer(AtomicBuffer &buffer) : buffer_{&buffer}
	{
		receiver_.emplace(*buffer_);
	}
	Consumer(Consumer &&other) noexcept : buffer_{other.buffer_}
	{
		receiver_.emplace(*buffer_);
	}

	template <class F>
	ReadStatus read(F &&fn)
	{
		if (!receiver_->receiveNext())
			return ReadStatus::Empty;
		/* receiveNext skipped ahead */
		if (receiver_->lappedCount() != 0)
			return ReadStatus::Lapped;

		const auto length = static_cast<std::size_t>(receiver_->length());
		const auto *msg = reinterpret_cast<const std::byte *>(
			receiver_->buffer().buffer() + receiver_->offset());
		fn(MessageHeader{length, static_cast<uint32_t>(receiver_->typeId())},
		   std::span{msg, length});

		return receiver_->validate() ? ReadStatus::Ok : ReadStatus::Lapped;
	}

	void respawn()
	{
		receiver_.emplace(*buffer_);
	}

private:
	AtomicBuffer *buffer_;
	/* BroadcastReceiver is neither copyable nor movable */
	std::optional<BroadcastReceiver> receiver_;
};
static_assert(MultiQueues::Consumer<Consumer>);

struct AlignedFree {
	void operator()(std::byte *p) const
	{
		::operator delete[](p, std::align_val_t{CACHE_LINE});
	}
};

/*
 * Args: queue capacity, number of readers, payload bytes (>= 8). Capacity
 * excludes Aeron's trailer.
 */
static void BM_Aeron_Broadcast_Throughput(benchmark::State &state)
{
	const auto capacity = static_cast<std::size_t>(state.range(0));
	const auto nr_readers = static_cast<std::size_t>(state.range(1));
	const auto size = static_cast<std::size_t>(state.range(2));

	const std::size_t length =
		capacity + BroadcastBufferDescriptor::TRAILER_LENGTH;
	const std::unique_ptr<std::byte[], AlignedFree> memory{
		new (std::align_val_t{CACHE_LINE}) std::byte[length]()};
	AtomicBuffer buffer{reinterpret_cast<std::uint8_t *>(memory.get()), length};
	BroadcastTransmitter transmitter{buffer};

	run_throughput(
		state, nr_readers, size, [&buffer] { return Consumer{buffer}; },
		[&transmitter](const std::span<const std::byte> payload) {
			/*
			 * transmit() copies from a source buffer; wrapping the
			 * payload is just a pointer + length.
			 */
			AtomicBuffer src{reinterpret_cast<std::uint8_t *>(
								 const_cast<std::byte *>(payload.data())),
							 payload.size()};
			transmitter.transmit(
				1, src, 0, static_cast<aeron::util::index_t>(payload.size()));
		});
}
}

BENCHMARK(BM_SPMC_Throughput)
	->ArgNames({"capacity", "readers", "bytes"})
	->ArgsProduct({{1 << 20}, benchmark::CreateDenseRange(2, 7, 1), {73}})
	->UseRealTime();
BENCHMARK(aeron_broadcast::BM_Aeron_Broadcast_Throughput)
	->ArgNames({"capacity", "readers", "bytes"})
	->ArgsProduct({{1 << 20}, benchmark::CreateDenseRange(2, 7, 1), {73}})
	->UseRealTime();
}

BENCHMARK_MAIN();
