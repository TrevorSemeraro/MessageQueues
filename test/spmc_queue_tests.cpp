#include <multiqueues/spmc.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <new>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

namespace MultiQueues {
bool write_int(SPMCProducer &p, int v)
{
	return p.write(sizeof(v), 1, [&](std::span<std::byte> buf) {
		std::memcpy(buf.data(), &v, sizeof(v));
	});
}

ReadStatus read_int(SPMCConsumer &c, int &out)
{
	return c.read([&](MessageHeader, std::span<std::byte> buf) {
		std::memcpy(&out, buf.data(), sizeof(out));
	});
}

class SPMCTest : public testing::Test {
protected:
	SPMC queue = SPMC::create(Heap{}, 1 << 20);
};

TEST_F(SPMCTest, SingleThreadSmoke)
{
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};
	auto consumer2{create_consumer(queue)};

	int v = 5;
	producer.write(sizeof(v), 1,
				   [&](std::span<std::byte> buf) { new (buf.data()) int{v}; });

	int vi;
	while (consumer.read([&](MessageHeader hdr, std::span<std::byte> buf) {
		switch (hdr.type) {
		case 1: {
			std::memcpy(&vi, buf.data(), sizeof(vi));
			EXPECT_EQ(vi, v);
			break;
		}
		default:
			throw std::runtime_error("Unexpected header type");
		}
	}) != ReadStatus::Ok) {
	}

	EXPECT_EQ(consumer.read([](MessageHeader, std::span<std::byte>) {}),
			  ReadStatus::Empty);
}

TEST_F(SPMCTest, SingleThreadRespawn)
{
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};

	int v = 5;
	producer.write(sizeof(v), 1,
				   [&](std::span<std::byte> buf) { new (buf.data()) int{v}; });
	consumer.respawn();
	v = 6;
	producer.write(sizeof(v), 1,
				   [&](std::span<std::byte> buf) { new (buf.data()) int{v}; });

	int vi;
	while (consumer.read([&](MessageHeader hdr, std::span<std::byte> buf) {
		switch (hdr.type) {
		case 1: {
			std::memcpy(&vi, buf.data(), sizeof(vi));
			std::cout << "read: " << vi << '\n';
			EXPECT_EQ(vi, v);
			break;
		}
		default:
			throw std::runtime_error("Unexpected header type");
		}
	}) != ReadStatus::Ok) {
	}

	EXPECT_EQ(consumer.read([](MessageHeader, std::span<std::byte>) {}),
			  ReadStatus::Empty);
}

TEST_F(SPMCTest, OrderConsistentAcrossConsumers)
{
	auto producer{create_producer(queue)};
	std::atomic<int> ready{0};

	constexpr size_t N = 32;
	std::vector<int> seen1, seen2;
	seen1.reserve(N);
	seen2.reserve(N);

	std::thread t1([this, &ready, &seen1]() mutable {
		auto consumer{create_consumer(queue)};
		ready.fetch_add(1);

		int v = 0;
		while (seen1.size() < N)
			if (read_int(consumer, v) == ReadStatus::Ok)
				seen1.push_back(v);
			else
				std::this_thread::yield();
	});

	std::thread t2([this, &ready, &seen2]() mutable {
		auto consumer2{create_consumer(queue)};
		ready.fetch_add(1);
		int v = 0;
		/* start slightly later than t1 */
		std::this_thread::sleep_for(std::chrono_literals::operator""us(200));
		while (seen2.size() < N)
			if (read_int(consumer2, v) == ReadStatus::Ok)
				seen2.push_back(v);
			else
				std::this_thread::yield();
	});

	while (ready.load() < 2)
		std::this_thread::yield();
	for (size_t i = 0; i < N; ++i) {
		while (!write_int(producer, static_cast<int>(i)))
			std::this_thread::yield();
		/* tiny delay to vary interleaving without risking overlap */
		std::this_thread::sleep_for(std::chrono_literals::operator""us(50));
	}

	t1.join();
	t2.join();

	for (size_t i = 0; i < N; ++i) {
		ASSERT_EQ(seen1[i], static_cast<int>(i));
		ASSERT_EQ(seen2[i], static_cast<int>(i));
	}
}

class SlowSPMC : public testing::Test {
protected:
	/*
	 * 1 KiB holds seven 128-byte frames per lap, so 32 writes lap a
	 * consumer that stops reading.
	 */
	SPMC queue = SPMC::create(Heap{}, 1 << 10);
};

TEST_F(SlowSPMC, SlowConsumerLapError)
{
	constexpr size_t N = 32;
	auto producer{create_producer(queue)};

	std::thread t1([this]() mutable {
		auto consumer{create_consumer(queue)};
		size_t seen1 = 0;
		int v = 0;
		while (seen1 < N)
			if (read_int(consumer, v) == ReadStatus::Ok)
				seen1++;
			else
				std::this_thread::yield();
	});

	std::thread t2([this]() mutable {
		auto consumer2{create_consumer(queue)};
		int v = 0;
		read_int(consumer2, v);
		std::this_thread::sleep_for(std::chrono_literals::operator""ms(10));
		EXPECT_EQ(read_int(consumer2, v), ReadStatus::Lapped);
	});

	for (size_t i = 0; i < N; ++i) {
		write_int(producer, static_cast<int>(i));
		std::this_thread::sleep_for(std::chrono_literals::operator""us(1));
	}

	t1.join();
	t2.join();
}

TEST(SPMCRegionTest, AttachSharesTheQueue)
{
	constexpr std::size_t capacity = 1 << 12;
	alignas(CACHE_LINE) static std::byte region[SPMC::region_size(capacity)];

	SPMC created = SPMC::create(Region{region, sizeof(region)}, capacity);
	auto producer{create_producer(created)};
	/* before anyone attaches */
	ASSERT_TRUE(write_int(producer, 1));

	SPMC attached = SPMC::attach(Region{region, sizeof(region)});
	/* sees only what's written from here on */
	auto consumer{create_consumer(attached)};
	ASSERT_TRUE(write_int(producer, 2));

	int v = 0;
	ASSERT_EQ(read_int(consumer, v), ReadStatus::Ok);
	EXPECT_EQ(v, 2);
	EXPECT_EQ(read_int(consumer, v), ReadStatus::Empty);
}

TEST(SPMCRegionTest, AttachRejectsTruncatedRegion)
{
	constexpr std::size_t capacity = 1 << 12;
	alignas(CACHE_LINE) static std::byte region[SPMC::region_size(capacity)];
	SPMC created = SPMC::create(Region{region, sizeof(region)}, capacity);
	EXPECT_THROW(SPMC::attach(Region{region, sizeof(region) / 2}),
				 std::runtime_error);
}
}
