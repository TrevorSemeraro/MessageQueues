#include <multiqueues/mpsc.hpp>
#include <multiqueues/spmc.hpp>
#include <multiqueues/spsc.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace MultiQueues {
namespace {
constexpr std::size_t CAPACITY = 1 << 12;

template <Producer P>
bool write_u64(P &producer, uint64_t value, std::size_t size = sizeof(uint64_t))
{
	return producer.write(size, 1, [&](std::span<std::byte> payload) {
		std::memcpy(payload.data(), &value, sizeof(value));
	});
}

template <Consumer C>
ReadStatus read_u64(C &consumer, uint64_t &value, std::size_t *size = nullptr)
{
	return consumer.read(
		[&](MessageHeader header, std::span<const std::byte> payload) {
			std::memcpy(&value, payload.data(), sizeof(value));
			if (size)
				*size = header.size;
		});
}

/* Zeroed, cache-line aligned caller memory. */
struct TestRegion {
	explicit TestRegion(std::size_t bytes) : lines(bytes / CACHE_LINE)
	{
	}

	Region region()
	{
		return Region{reinterpret_cast<std::byte *>(lines.data()),
					  lines.size() * CACHE_LINE};
	}

	std::vector<detail::Line> lines;
};

/* A name no other test or run uses. */
template <class Queue>
SharedMemory shm_name(const char *test)
{
	return SharedMemory{"/multiqueues-test-" + std::to_string(getpid()) + "-" +
						std::to_string(Queue::policy_type::policy_id) + "-" +
						test};
}
}

template <class Queue>
class QueueContract : public testing::Test {};
using Queues = testing::Types<SPSC, SPMC, MPSC>;
TYPED_TEST_SUITE(QueueContract, Queues);

TYPED_TEST(QueueContract, RoundTripsOneMessage)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};

	ASSERT_TRUE(
		producer.write(sizeof(int), 7, [](std::span<std::byte> payload) {
			::new (payload.data()) int{42};
		}));

	MessageHeader header{};
	int value = 0;
	ASSERT_EQ(
		consumer.read([&](MessageHeader h, std::span<const std::byte> payload) {
			header = h;
			std::memcpy(&value, payload.data(), sizeof(value));
		}),
		ReadStatus::Ok);
	EXPECT_EQ(header, (MessageHeader{sizeof(int), 7}));
	EXPECT_EQ(value, 42);

	uint64_t unused;
	EXPECT_EQ(read_u64(consumer, unused), ReadStatus::Empty);
}

TYPED_TEST(QueueContract, WrapsAroundWithMixedSizes)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};
	const std::size_t largest =
		std::min<std::size_t>(producer.max_payload(), 300);

	auto size_of = [&](uint64_t i) {
		return sizeof(uint64_t) + (i * 37) % (largest - sizeof(uint64_t) + 1);
	};

	uint64_t next_expected = 0;
	auto drain = [&] {
		uint64_t value;
		std::size_t size;
		while (read_u64(consumer, value, &size) == ReadStatus::Ok) {
			ASSERT_EQ(value, next_expected);
			ASSERT_EQ(size, size_of(next_expected));
			++next_expected;
		}
	};

	for (uint64_t i = 0; i < 2000; ++i) {
		ASSERT_TRUE(write_u64(producer, i, size_of(i)));
		if (i % 3 == 2)
			drain();
	}
	drain();
	EXPECT_EQ(next_expected, 2000u);
}

TYPED_TEST(QueueContract, AcceptsMaxPayloadWhenEmpty)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	auto consumer{create_consumer(queue)};
	uint64_t value;

	for (uint64_t i = 0; i < 10; ++i) {
		ASSERT_TRUE(write_u64(producer, i, producer.max_payload()));
		ASSERT_EQ(read_u64(consumer, value), ReadStatus::Ok);
		ASSERT_EQ(value, i);
	}
}

TYPED_TEST(QueueContract, RejectsOversizedPayload)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	EXPECT_THROW(static_cast<void>(producer.write(producer.max_payload() + 1, 1,
												  [](std::span<std::byte>) {})),
				 std::invalid_argument);
}

TYPED_TEST(QueueContract, RejectsReservedType)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	auto producer{create_producer(queue)};
	EXPECT_THROW(static_cast<void>(
					 producer.write(8, std::numeric_limits<uint32_t>::max(),
									[](std::span<std::byte>) {})),
				 std::invalid_argument);
}

TYPED_TEST(QueueContract, RejectsInvalidCapacity)
{
	EXPECT_THROW(TypeParam::create(Heap{}, CAPACITY + 1),
				 std::invalid_argument);
	EXPECT_THROW(TypeParam::create(Heap{}, CACHE_LINE), std::invalid_argument);
}

TYPED_TEST(QueueContract, SingleRolesAreExclusive)
{
	auto queue = TypeParam::create(Heap{}, CAPACITY);
	using Policy = typename TypeParam::policy_type;
	{
		auto producer{create_producer(queue)};
		if constexpr (Policy::single_producer)
			EXPECT_THROW(static_cast<void>(create_producer(queue)),
						 std::logic_error);
		else
			EXPECT_NO_THROW(static_cast<void>(create_producer(queue)));

		auto consumer{create_consumer(queue)};
		if constexpr (Policy::single_consumer)
			EXPECT_THROW(static_cast<void>(create_consumer(queue)),
						 std::logic_error);
		else
			EXPECT_NO_THROW(static_cast<void>(create_consumer(queue)));
	}
	EXPECT_NO_THROW(static_cast<void>(create_producer(queue)));
	EXPECT_NO_THROW(static_cast<void>(create_consumer(queue)));
}

TYPED_TEST(QueueContract, AttachSharesTheQueue)
{
	TestRegion memory{TypeParam::region_size(CAPACITY)};
	auto created = TypeParam::create(memory.region(), CAPACITY);
	auto attached = TypeParam::attach(memory.region());
	EXPECT_EQ(attached.capacity(), CAPACITY);

	auto producer{create_producer(created)};
	auto consumer{create_consumer(attached)};
	ASSERT_TRUE(write_u64(producer, 9));

	uint64_t value = 0;
	ASSERT_EQ(read_u64(consumer, value), ReadStatus::Ok);
	EXPECT_EQ(value, 9u);
}

TYPED_TEST(QueueContract, AttachRejectsUninitialisedRegion)
{
	TestRegion memory{TypeParam::region_size(CAPACITY)};
	EXPECT_THROW(TypeParam::attach(memory.region()), std::runtime_error);
}

TYPED_TEST(QueueContract, AttachRejectsAnotherQueueType)
{
	using Other =
		std::conditional_t<std::is_same_v<TypeParam, SPSC>, SPMC, SPSC>;
	TestRegion memory{std::max(TypeParam::region_size(CAPACITY),
							   Other::region_size(CAPACITY))};
	auto created = Other::create(memory.region(), CAPACITY);
	EXPECT_THROW(TypeParam::attach(memory.region()), std::runtime_error);
}

TYPED_TEST(QueueContract, SharedMemoryCreateFailsIfNameExists)
{
	const SharedMemory shm = shm_name<TypeParam>("exists");
	auto created = TypeParam::create(shm, CAPACITY);
	EXPECT_THROW(TypeParam::create(shm, CAPACITY), std::system_error);
}

TYPED_TEST(QueueContract, SharedMemoryAttachSharesTheQueue)
{
	const SharedMemory shm = shm_name<TypeParam>("shares");
	auto created = TypeParam::create(shm, CAPACITY);
	auto attached = TypeParam::attach(shm);
	EXPECT_EQ(attached.capacity(), CAPACITY);

	auto consumer{create_consumer(attached)};
	auto producer{create_producer(created)};
	ASSERT_TRUE(write_u64(producer, 11));

	uint64_t value = 0;
	ASSERT_EQ(read_u64(consumer, value), ReadStatus::Ok);
	EXPECT_EQ(value, 11u);
}

TYPED_TEST(QueueContract, SharedMemoryCreatorUnlinksOnDestruction)
{
	const SharedMemory shm = shm_name<TypeParam>("unlink");
	{
		auto created = TypeParam::create(shm, CAPACITY);
	}
	EXPECT_THROW(TypeParam::attach(shm), std::system_error);
}

TYPED_TEST(QueueContract, ReclaimsRolesFromDeadProcess)
{
	const SharedMemory shm = shm_name<TypeParam>("reclaim");
	auto queue = TypeParam::create(shm, CAPACITY);

	const pid_t child = fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		/* Take every role, then crash without releasing them. */
		try {
			auto attached = TypeParam::attach(shm);
			auto producer{create_producer(attached)};
			auto consumer{create_consumer(attached)};
			kill(getpid(), SIGKILL);
		} catch (...) {
		}
		_exit(1);
	}

	int status = 0;
	ASSERT_EQ(waitpid(child, &status, 0), child);
	ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

	EXPECT_NO_THROW(static_cast<void>(create_producer(queue)));
	EXPECT_NO_THROW(static_cast<void>(create_consumer(queue)));
}
}
