#pragma once
#include "abstract_queue.hpp"

namespace MultiQueues {
    namespace detail {
        struct MPMCHeader {
            HeaderPrefix prefix;
            alignas(cache_line) std::atomic<uint64_t> read_counter{0};
            alignas(cache_line) std::atomic<uint64_t> write_counter{0};
        };
    }

    class MPMCProducer {
    public:
        using view_type = QueueView<detail::MPMCHeader>;

        template <std::invocable<std::span<std::byte>> F>
        uint8_t write(std::size_t size, uint32_t type, F fn) {

        }
    private:
        template <class Policy, class Allocator>
        friend class AbstractQueue;

        explicit MPMCProducer(const QueueView queue)
        : queue_{queue}, cached_read_counter_{queue.header->read_counter.load(std::memory_order_acquire)} {}

        QueueView queue_;
    };

    class MPMCConsumer {
    public:
        template <std::invocable<MessageHeader, std::span<std::byte>> F>
        ReadStatus read(F&& fn) {

        }
    private:
        template <class Policy, class Allocator>
        friend class AbstractQueue;

        explicit MPMCConsumer(const QueueView queue)
        : queue_{queue}, cached_write_counter_{queue.header->write_counter.load(std::memory_order_acquire)} {}

        QueueView queue_;
    };

    struct MPMCPolicy {
        using producer_type = MPMCProducer;
        using consumer_type = MPMCConsumer;
        using header_type = detail::MPMCHeader;
        static constexpr uint64_t policy_id = 2;
    };

    // this should inherit from abstract queue and implement the header alloc itself
    template <class Allocator = std::allocator<detail::Line>>
    using MPMC = AbstractQueue<MPMCPolicy, Allocator>;
};