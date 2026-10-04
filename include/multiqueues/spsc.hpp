#pragma once
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <utility>
#include <variant>

#include "abstract_queue.hpp"

namespace MultiQueues {
    namespace detail {
        struct WritePosition {
            uint64_t pos;
            bool wrap;
        };
    }
    using namespace detail;

    class SPSCProducer {
    public:
        template <std::invocable<std::span<std::byte>> F> uint8_t write(std::size_t size, uint32_t type, F fn) {
            const uint64_t w = queue_.header->write_counter.load(std::memory_order_acquire);
            const std::size_t FrameSize = frame_size(size);

            auto wp = write_position(w, FrameSize);
            if (!wp) {
                cached_read_counter_ = queue_.header->read_counter.load(std::memory_order_acquire);
                wp = write_position(w, FrameSize);
                if (!wp) return false;
            }

            if (wp->wrap) ::new (queue_.data + w) MessageHeader{SpanHeader};

            std::byte* slot = queue_.data + wp->pos;
            ::new (slot) MessageHeader{size, type};
            fn(std::span{slot + PayloadOffset, size});

            queue_.header->write_counter.store(wp->pos + FrameSize, std::memory_order_release);
            return true;
        }

    private:
        template <class Policy, class Allocator> friend class AbstractQueue;

        explicit SPSCProducer(const QueueView queue)
            : queue_{queue}, cached_read_counter_{queue.header->read_counter.load(std::memory_order_acquire)} {}

        [[nodiscard]] std::optional<detail::WritePosition> write_position(const uint64_t w,
                                                                          const uint64_t frame_size) const {
            const uint64_t r = cached_read_counter_;
            if (w >= r) {
                if (w + frame_size + QueueHeaderSize > queue_.capacity) {
                    if (frame_size >= r) return std::nullopt;
                    return detail::WritePosition{0, true};
                }
            } else if (w + frame_size >= r) {
                return std::nullopt;
            }
            return detail::WritePosition{w, false};
        }

        QueueView queue_;
        alignas(cache_line) uint64_t cached_read_counter_;
    };

    class SPSCConsumer {
    public:
        template <std::invocable<MessageHeader, std::span<std::byte>> F> ReadStatus read(F&& fn) {
            uint64_t r = queue_.header->read_counter.load(std::memory_order_acquire);

            if (r == cached_write_counter_) {
                cached_write_counter_ = queue_.header->write_counter.load(std::memory_order_acquire);
                if (r == cached_write_counter_) return ReadStatus::Empty;
            }

            MessageHeader header{};
            std::memcpy(&header, queue_.data + r, sizeof(header));
            if (header == SpanHeader) {
                r = 0;
                std::memcpy(&header, queue_.data + r, sizeof(header));
            }

            fn(header, std::span{queue_.data + r + PayloadOffset, header.size});

            queue_.header->read_counter.store(r + frame_size(header.size), std::memory_order_release);
            return ReadStatus::Ok;
        }

    private:
        template <class Policy, class Allocator> friend class AbstractQueue;

        explicit SPSCConsumer(const QueueView queue)
            : queue_{queue}, cached_write_counter_{queue.header->write_counter.load(std::memory_order_acquire)} {}

        QueueView queue_;
        alignas(cache_line) uint64_t cached_write_counter_;
    };

    struct SPSCPolicy {
        using producer_type = SPSCProducer;
        using consumer_type = SPSCConsumer;
    };
    static_assert(Producer<SPSCProducer> && Consumer<SPSCConsumer>);

    template <class Allocator = std::allocator<detail::Line>> using SPSC = AbstractQueue<SPSCPolicy, Allocator>;
};