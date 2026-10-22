#include <multiqueues/mpsc.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

namespace MultiQueues {
namespace {
constexpr std::size_t CAPACITY = 1 << 10;
constexpr std::size_t FRAMES_PER_CAPACITY = CAPACITY / detail::frame_size(8);

bool write_small(MPSCProducer &producer)
{
	return producer.write(8, 1, [](std::span<std::byte>) {});
}

ReadStatus read_any(MPSCConsumer &consumer)
{
	return consumer.read([](MessageHeader, std::span<const std::byte>) {});
}
}

TEST(MPSC, ReportsFullInsteadOfOverwriting)
{
	MPSC queue = MPSC::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};

	std::size_t written = 0;
	while (write_small(producer))
		++written;
	EXPECT_EQ(written, FRAMES_PER_CAPACITY);

	ASSERT_EQ(read_any(consumer), ReadStatus::Ok);
	EXPECT_TRUE(write_small(producer));
	EXPECT_FALSE(write_small(producer));
}

TEST(MPSC, SpaceIsFreedOnlyAfterTheReadCallbackReturns)
{
	MPSC queue = MPSC::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};

	for (std::size_t i = 0; i < FRAMES_PER_CAPACITY; ++i)
		ASSERT_TRUE(write_small(producer));

	ASSERT_EQ(consumer.read([&](MessageHeader, std::span<const std::byte>) {
		EXPECT_FALSE(write_small(producer));
	}),
			  ReadStatus::Ok);
	EXPECT_TRUE(write_small(producer));
}

TEST(MPSC, AllowsOnlyOneConsumer)
{
	MPSC queue = MPSC::create(Heap{}, CAPACITY);
	{
		auto consumer{create_consumer(queue)};
		EXPECT_THROW(static_cast<void>(create_consumer(queue)),
					 std::logic_error);
	}
	EXPECT_NO_THROW(static_cast<void>(create_consumer(queue)));
}

TEST(MPSC, ManyProducersDeliverEachMessageOnceInProducerOrder)
{
	constexpr uint64_t PER_PRODUCER = 10'000;
	constexpr uint64_t NR_PRODUCERS = 4;
	constexpr uint64_t TOTAL = PER_PRODUCER * NR_PRODUCERS;
	auto size_of = [](uint64_t value) {
		return sizeof(uint64_t) + value % 200;
	};
	MPSC queue = MPSC::create(Heap{}, 1 << 14);
	auto consumer{create_consumer(queue)};

	std::vector<std::thread> producers;
	for (uint64_t p = 0; p < NR_PRODUCERS; ++p) {
		producers.emplace_back(
			[producer = create_producer(queue), p, size_of]() mutable {
				for (uint64_t i = 0; i < PER_PRODUCER; ++i) {
					const uint64_t value = p * PER_PRODUCER + i;
					while (!producer.write(
						size_of(value), 1, [&](std::span<std::byte> buf) {
							std::memcpy(buf.data(), &value, sizeof(value));
						})) {
					}
				}
			});
	}

	std::vector<uint64_t> next(NR_PRODUCERS);
	std::vector<uint64_t> all;
	all.reserve(TOTAL);
	while (all.size() < TOTAL) {
		uint64_t value = 0;
		std::size_t size = 0;
		if (consumer.read(
				[&](MessageHeader header, std::span<const std::byte> buf) {
					std::memcpy(&value, buf.data(), sizeof(value));
					size = header.size;
				}) != ReadStatus::Ok)
			continue;
		ASSERT_EQ(size, size_of(value));
		const uint64_t p = value / PER_PRODUCER;
		ASSERT_EQ(value % PER_PRODUCER, next[p]++);
		all.push_back(value);
	}
	for (auto &producer : producers)
		producer.join();

	std::ranges::sort(all);
	for (uint64_t i = 0; i < TOTAL; ++i)
		ASSERT_EQ(all[i], i);
	EXPECT_EQ(read_any(consumer), ReadStatus::Empty);
}
}
