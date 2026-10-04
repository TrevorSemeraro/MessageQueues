#include <multiqueues/spsc.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <thread>

namespace MultiQueues {
    class SPSCTest : public testing::Test {
    protected:
        MultiQueues::SPSC<> queue{1 << 20};
    };

    TEST_F(SPSCTest, ProducerConsumerIntAndFloat) {
        constexpr int kCount = 100'000;

        std::thread producer([this] {
            auto producer{queue.get_producer()};
            for (int i = 0; i < kCount; ++i) {
                if (i % 2 == 0) {
                    int v = i;
                    while (!producer.write(sizeof(v), 1, [&](std::span<std::byte> buf) {
                        new (buf.data()) int{v}; // write directly in queue, no object copy
                    })) {
                    }
                } else {
                    float v = static_cast<float>(i) + 0.5f;
                    while (!producer.write(sizeof(v), 2, [&](std::span<std::byte> buf) {
                        new (buf.data()) float{v}; // write directly in queue, no object copy
                    })) {
                    }
                }
            }
        });

        std::thread consumer([this] {
            auto consumer{queue.get_consumer()};

            for (int i = 0; i < kCount; ++i) {
                int vi;
                float vf;
                while (consumer.read([&](MessageHeader hdr, std::span<std::byte> buf) {
                    switch (hdr.type) {
                    case 1: {
                        std::memcpy(&vi, buf.data(), sizeof(vi));
                        EXPECT_EQ(vi, i);
                        break;
                    }
                    case 2: {
                        std::memcpy(&vf, buf.data(), sizeof(vf));
                        EXPECT_EQ(vf, static_cast<float>(i) + 0.5f);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unexpected header type");
                    }
                }) != ReadStatus::Ok) {
                }
            }
        });

        producer.join();
        consumer.join();
    }
};
