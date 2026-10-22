#pragma once
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "concepts.hpp"

namespace MultiQueues {
/* Process-private memory from the heap. */
struct Heap {};

/*
 * A POSIX shared memory object such as "/orders", for queues shared between
 * processes. The queue that creates it unlinks it when destroyed.
 */
struct SharedMemory {
	std::string name;
};

/* Caller-owned, cache-line aligned memory. The queue never frees it. */
struct Region {
	std::byte *data;
	std::size_t size;
};

namespace detail {
[[noreturn]] inline void throw_errno(const std::string &what)
{
	throw std::system_error(errno, std::generic_category(),
							"MultiQueues: " + what);
}

/* The memory a queue lives in; releases it according to where it came from. */
class Mapping {
public:
	static Mapping create(Heap, const std::size_t size)
	{
		auto *data = static_cast<std::byte *>(
			::operator new(size, std::align_val_t{CACHE_LINE}));
		return Mapping{Kind::Heap, data, size};
	}

	/* Fails with EEXIST rather than reuse a name a live queue may own. */
	static Mapping create(SharedMemory shm, const std::size_t size)
	{
		const int fd =
			shm_open(shm.name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
		if (fd < 0)
			throw_errno("shm_open " + shm.name);

		if (ftruncate(fd, static_cast<off_t>(size))) {
			const int err = errno;
			close(fd);
			shm_unlink(shm.name.c_str());
			errno = err;
			throw_errno("ftruncate " + shm.name);
		}

		void *data = map(fd, size);
		if (!data) {
			const int err = errno;
			shm_unlink(shm.name.c_str());
			errno = err;
			throw_errno("mmap " + shm.name);
		}

		Mapping mapping{Kind::Shared, static_cast<std::byte *>(data), size};
		mapping.unlink_ = std::move(shm.name);
		return mapping;
	}

	static Mapping create(const Region region, const std::size_t size)
	{
		if (region.size < size)
			throw std::invalid_argument(
				"MultiQueues: region is smaller than region_size(capacity)");
		return attach(region);
	}

	static Mapping attach(const SharedMemory &shm)
	{
		const int fd = shm_open(shm.name.c_str(), O_RDWR, 0);
		if (fd < 0)
			throw_errno("shm_open " + shm.name);

		struct stat st;
		if (fstat(fd, &st)) {
			const int err = errno;
			close(fd);
			errno = err;
			throw_errno("fstat " + shm.name);
		}

		const auto size = static_cast<std::size_t>(st.st_size);
		void *data = map(fd, size);
		if (!data)
			throw_errno("mmap " + shm.name);
		return Mapping{Kind::Shared, static_cast<std::byte *>(data), size};
	}

	static Mapping attach(const Region region)
	{
		if (reinterpret_cast<std::uintptr_t>(region.data) % CACHE_LINE)
			throw std::invalid_argument(
				"MultiQueues: region must be cache-line aligned");
		return Mapping{Kind::Region, region.data, region.size};
	}

	Mapping(Mapping &&other) noexcept
		: kind_{other.kind_}, data_{std::exchange(other.data_, nullptr)},
		  size_{other.size_}, unlink_{std::move(other.unlink_)}
	{
	}

	Mapping &operator=(Mapping &&other) noexcept
	{
		if (this != &other) {
			release();
			kind_ = other.kind_;
			data_ = std::exchange(other.data_, nullptr);
			size_ = other.size_;
			unlink_ = std::move(other.unlink_);
		}
		return *this;
	}

	~Mapping()
	{
		release();
	}

	[[nodiscard]] std::byte *data() const
	{
		return data_;
	}

	[[nodiscard]] std::size_t size() const
	{
		return size_;
	}

private:
	enum class Kind { Heap, Shared, Region };

	Mapping(const Kind kind, std::byte *data, const std::size_t size)
		: kind_{kind}, data_{data}, size_{size}
	{
	}

	/* Maps the whole object and faults it in now; closes fd either way. */
	static void *map(const int fd, const std::size_t size)
	{
		void *data = mmap(nullptr, size, PROT_READ | PROT_WRITE,
						  MAP_SHARED | MAP_POPULATE, fd, 0);
		const int err = errno;
		close(fd);
		errno = err;
		return data == MAP_FAILED ? nullptr : data;
	}

	void release()
	{
		if (!data_)
			return;

		switch (kind_) {
		case Kind::Heap:
			::operator delete(data_, std::align_val_t{CACHE_LINE});
			break;
		case Kind::Shared:
			munmap(data_, size_);
			if (!unlink_.empty())
				shm_unlink(unlink_.c_str());
			break;
		case Kind::Region:
			break;
		}
		data_ = nullptr;
	}

	Kind kind_;
	std::byte *data_;
	std::size_t size_;
	std::string unlink_; /* set only on the creator of a shared object */
};
}
}
