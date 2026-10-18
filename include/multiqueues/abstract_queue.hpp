#pragma once
#include <atomic>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <cassert>

namespace MultiQueues {
    namespace detail {
        inline constexpr uint64_t ProtocolMajor = 1;
        inline constexpr uint64_t ProtocolMinor = 0;

#ifdef __cpp_lib_hardware_interference_size
        constexpr std::size_t cache_line = std::hardware_destructive_interference_size;
#else
        constexpr std::size_t cache_line = 64;
#endif
        struct alignas(detail::cache_line) Line {
            std::byte b[detail::cache_line];
        };

        template <uint64_t A> constexpr uint64_t align_as(const uint64_t n) {
            static_assert((A & (A - 1)) == 0, "A must be power of two");
            return (n + (A - 1)) & ~(A - 1);
        }

        struct QueueHeader {
            uint64_t protocol_major;
            uint64_t protocol_minor;
            uint64_t capacity;
            alignas(cache_line) std::atomic<uint64_t> read_counter{0};
            alignas(cache_line) std::atomic<uint64_t> write_counter{0};
        };
        // The counters may live in memory shared between processes.
        static_assert(std::atomic<uint64_t>::is_always_lock_free);

        struct QueueView {
            QueueHeader* header{nullptr};
            std::byte* data{nullptr};
            uint64_t capacity{0}; // bytes in the data region; always a power of two

            // Position of a running byte counter within the data region.
            [[nodiscard]] uint64_t offset_of(const uint64_t counter) const { return counter & (capacity - 1); }
        };

        struct MessageHeader {
            uint64_t size;
            uint32_t type;

            bool operator==(const MessageHeader&) const = default;
        };

        // Written where a frame would not fit before the end of the buffer: "continue reading at offset 0".
        inline constexpr MessageHeader SpanHeader{0, std::numeric_limits<uint32_t>::max()};
        inline constexpr uint64_t QueueHeaderSize = align_as<cache_line>(sizeof(QueueHeader));

        // Payloads start on their own cache line, so any type up to alignas(cache_line) can be placed in them.
        inline constexpr uint64_t PayloadOffset = align_as<cache_line>(sizeof(MessageHeader));

        inline uint64_t frame_size(std::size_t size) { return align_as<cache_line>(PayloadOffset + size); }

        struct WriteFnArchetype {
            void* state;
            void operator()(std::span<std::byte>) const {}
        };

        struct ReadFnArchetype {
            void* state;
            void operator()(MessageHeader, std::span<std::byte>) const {}
        };
    };

    enum class ReadStatus : uint8_t {
        Empty,
        Ok,
        Lapped,
    };

    template <class P>
    concept Producer = requires(P p, std::size_t size, uint32_t type, detail::WriteFnArchetype fn) {
        { p.write(size, type, fn) } -> std::same_as<uint8_t>;
    };
    template <class C>
    concept Consumer = requires(C c, detail::ReadFnArchetype buf) {
        { c.read(buf) } -> std::same_as<ReadStatus>;
    };

    template <class Policy, class Allocator = std::allocator<detail::Line>> class AbstractQueue {
    public:
        using allocator_type = Allocator;
        using value_type = std::byte;

        explicit AbstractQueue(const std::size_t capacity, Allocator alloc = Allocator{}) : alloc_{alloc} {
            assert((capacity & (capacity - 1)) == 0); // capacity power of 2
            assert(capacity >= detail::cache_line); // capacity >= cache line size

            blocks_ = (detail::QueueHeaderSize + capacity) / detail::cache_line;
            region_ = reinterpret_cast<std::byte*>(alloc_.allocate(blocks_));
            init(capacity);
        }

        enum class Open { Create, Attach };

        // Bytes a queue of `capacity` occupies in caller-provided memory.
        static constexpr std::size_t region_size(const std::size_t capacity) {
            return detail::QueueHeaderSize + capacity;
        }

        // A queue in caller-owned memory of region_size(capacity) bytes, cache-line aligned: e.g. a
        // shared-memory mapping, so producers and consumers can live in different processes. Create
        // initialises a new queue there; Attach joins one already initialised (by any process),
        // throwing if it isn't a queue of this protocol and capacity. The memory is not freed.
        AbstractQueue(std::byte* region, const std::size_t capacity, const Open open) : region_{region} {
            assert((capacity & (capacity - 1)) == 0);
            assert(capacity >= detail::cache_line);
            assert(reinterpret_cast<std::uintptr_t>(region) % detail::cache_line == 0);

            if (open == Open::Create) {
                init(capacity);
                return;
            }
            auto* header = reinterpret_cast<detail::QueueHeader*>(region_);
            if (header->protocol_major != detail::ProtocolMajor || header->capacity != capacity)
                throw std::runtime_error("MultiQueues: region does not hold a queue of this protocol/capacity");
            view_ = detail::QueueView{header, region_ + detail::QueueHeaderSize, capacity};
        }

        ~AbstractQueue() {
            if (blocks_) alloc_.deallocate(reinterpret_cast<detail::Line*>(region_), blocks_);
        }

        Policy::producer_type get_producer() { return typename Policy::producer_type{view_}; };
        // Reads only frames published after this call.
        Policy::consumer_type get_consumer() {
            return typename Policy::consumer_type{view_};
        }

        [[nodiscard]] std::size_t capacity() const { return view_.capacity; }

        AbstractQueue(const AbstractQueue&) = delete;
        AbstractQueue& operator=(const AbstractQueue&) = delete;
    private:
        void init(const std::size_t capacity) {
            auto* header = ::new (region_) detail::QueueHeader{
                .protocol_major = detail::ProtocolMajor,
                .protocol_minor = detail::ProtocolMinor,
                .capacity = capacity,
            };
            view_ = detail::QueueView{header, region_ + detail::QueueHeaderSize, capacity};
        }

        [[no_unique_address]] Allocator alloc_;
        size_t blocks_ = 0; // 0: caller-owned memory, not ours to free

        std::byte* region_;
        detail::QueueView view_;
    };
}
