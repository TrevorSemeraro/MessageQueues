#pragma once
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <utility>
#include <variant>

#include "abstract_queue.hpp"

namespace MultiQueues {
    using namespace detail;

    class SPMCProducer {
    public:
        template <std::invocable<std::span<std::byte>> F> uint8_t write(std::size_t size, uint32_t type, F fn) {
            const std::size_t FrameSize = frame_size(size);

            if (offset_ + FrameSize + QueueHeaderSize >= queue_.capacity) {
                published_ += queue_.capacity - queue_.offset_of(published_);
                queue_.header->write_counter.store(published_, std::memory_order_release);
                ::new (queue_.data + offset_) MessageHeader{SpanHeader};
                offset_ = 0;
            }

            published_ += FrameSize; // size after writing this block
            if (reserved_ < published_) {
                // TODO: increase counter by := Q BLOCK BYTES
                reserved_ = align_as<cache_line>(published_);
                queue_.header->write_counter.store(reserved_, std::memory_order_release);
            }

            ::new (queue_.data + offset_) MessageHeader{size, type};
            fn(std::span{queue_.data + offset_ + PayloadOffset, size});

            offset_ += FrameSize;
            queue_.header->read_counter.store(published_, std::memory_order_release);
            return 1;
        }

    private:
        template <class Policy, class Allocator> friend class AbstractQueue;

        explicit SPMCProducer(const QueueView& queue) : queue_{queue} {
            reserved_ = queue_.header->write_counter.load(std::memory_order_acquire);
            published_ = reserved_;
            offset_ = queue_.offset_of(published_);
        }

        QueueView queue_;
        alignas(cache_line) uint64_t published_; // total bytes made visible to consumers
        uint64_t offset_;                        // where the next frame goes in the data region
        uint64_t reserved_;                      // last value stored to write_counter
    };

    class SPMCConsumer {
    public:
        template <std::invocable<MessageHeader, std::span<std::byte>> F> ReadStatus read(F&& fn) {
            if (read_ == cached_published_) {
                cached_published_ = queue_.header->read_counter.load(std::memory_order_acquire);
                if (read_ == cached_published_) return ReadStatus::Empty;
            }

            MessageHeader header{};
            std::memcpy(&header, queue_.data + offset_, sizeof(header));

            uint64_t writeCounter = queue_.header->write_counter.load(std::memory_order_acquire);
            if (writeCounter - read_ >= queue_.capacity) return ReadStatus::Lapped;

            if (header == SpanHeader) {
                offset_ = 0;
                read_ += queue_.capacity - queue_.offset_of(read_);
                std::memcpy(&header, queue_.data + offset_, sizeof(header));
            }

            fn(header, std::span{queue_.data + offset_ + PayloadOffset, header.size});

            read_ += frame_size(header.size);
            offset_ += frame_size(header.size);

            writeCounter = queue_.header->write_counter.load(std::memory_order_acquire);
            if (writeCounter - read_ >= queue_.capacity) return ReadStatus::Lapped;
            return ReadStatus::Ok;
        }

        void respawn() {
            cached_published_ = queue_.header->read_counter.load(std::memory_order_acquire);
            read_ = cached_published_;
            offset_ = (read_) & (queue_.capacity - 1);
        }

    private:
        template <class Policy, class Allocator> friend class AbstractQueue;

        explicit SPMCConsumer(const QueueView queue) : queue_{queue} {
            cached_published_ = queue_.header->read_counter.load(std::memory_order_acquire);
            read_ = cached_published_;
            offset_ = queue_.offset_of(read_);
        }

        alignas(cache_line) uint64_t cached_published_;
        QueueView queue_;
        uint64_t offset_; // where the next frame is in the data region
        uint64_t read_;   // total bytes consumed; compared against write_counter to detect laps
    };

    struct SPMCPolicy {
        using producer_type = SPMCProducer;
        using consumer_type = SPMCConsumer;
    };
    static_assert(Producer<SPMCProducer> && Consumer<SPMCConsumer>);

    template <class Allocator = std::allocator<detail::Line>> using SPMC = AbstractQueue<SPMCPolicy, Allocator>;
}
