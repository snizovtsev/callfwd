#pragma once

#include <folly/hash/FarmHash.h>

// See also https://github.com/jermp/bench_hash_functions

namespace farmhash = folly::external::farmhash;

namespace pthash {

namespace util {

template <typename Hasher>
static inline void check_hash_collision_probability(uint64_t size) {
    /*
        Adapted from: https://preshing.com/20110504/hash-collision-probabilities.
        Given a universe of size U (total number of possible hash values),
        which is U = 2^b for b-bit hash codes,
        the collision probability for n keys is (approximately):
            1 - e^{-n(n-1)/(2U)}.
        For example, for U=2^32 (32-bit hash codes), this probability
        gets to 50% already for n = 77,163 keys.
        We can approximate 1-e^{-X} with X when X is sufficiently small.
        Then our collision probability is
            n(n-1)/(2U) ~ n^2/(2U).
        So it can derived that for ~1.97B keys and 64-bit hash codes,
        the probability of collision is ~0.1 (10%), which may not be
        so small for some applications.
        For n = 2^30, the probability of collision is ~0.031 (3.1%).
    */
    if (sizeof(typename Hasher::hash_type) * 8 == 64 and size > (1ULL << 30)) {
        throw std::runtime_error(
            "Using 64-bit hash codes with more than 2^30 keys can be dangerous due to "
            "collisions: use 128-bit hash codes instead.");
    }
}

}  // namespace util

struct byte_range {
    uint8_t const* begin;
    uint8_t const* end;
};

inline uint64_t default_hash64(uint64_t val, uint64_t seed) {
    return farmhash::Hash64WithSeed((const char *)&val, sizeof(uint64_t), seed);
}

struct hash64 {
    hash64() {}
    hash64(uint64_t hash) : m_hash(hash) {}

    inline uint64_t first() const {
        return m_hash;
    }

    inline uint64_t second() const {
        return m_hash;
    }

    inline uint64_t mix() const {
        // From: http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html
        // 13-th variant
        uint64_t z = m_hash;
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }

private:
    uint64_t m_hash;
};

struct hash128 {
    hash128() {}
    hash128(uint64_t first, uint64_t second) : m_first(first), m_second(second) {}

    inline uint64_t first() const {
        return m_first;
    }

    inline uint64_t second() const {
        return m_second;
    }

    inline uint64_t mix() const {
        return m_first ^ m_second;
    }

private:
    uint64_t m_first, m_second;
};

struct hash_64 {
    typedef hash64 hash_type;

    // generic range of bytes
    static inline hash64 hash(byte_range range, uint64_t seed) {
        return farmhash::Hash64WithSeed((const char*) range.begin, range.end - range.begin, seed);
    }

    // specialization for std::string
    static inline hash64 hash(std::string const& val, uint64_t seed) {
        return farmhash::Hash64WithSeed(val.data(), val.size(), seed);
    }

    // specialization for uint64_t
    static inline hash64 hash(uint64_t val, uint64_t seed) {
        return farmhash::Hash64WithSeed(reinterpret_cast<char const*>(&val), sizeof(val), seed);
    }
};

struct hash_128 {
    typedef hash128 hash_type;

    // generic range of bytes
    static inline hash128 hash(byte_range range, uint64_t seed) {
        return {farmhash::Hash64WithSeed((const char*) range.begin, range.end - range.begin, seed),
                farmhash::Hash64WithSeed((const char*) range.begin, range.end - range.begin, ~seed)};
    }

    // specialization for std::string
    static inline hash128 hash(std::string const& val, uint64_t seed) {
        return {farmhash::Hash64WithSeed(val.data(), val.size(), seed),
                farmhash::Hash64WithSeed(val.data(), val.size(), ~seed)};
    }

    // specialization for uint64_t
    static inline hash128 hash(uint64_t val, uint64_t seed) {
        return {farmhash::Hash64WithSeed(reinterpret_cast<char const*>(&val), sizeof(val), seed),
                farmhash::Hash64WithSeed(reinterpret_cast<char const*>(&val), sizeof(val), ~seed)};
    }
};

}  // namespace pthash
