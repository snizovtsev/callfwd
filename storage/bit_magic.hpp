#ifndef HIPERF_LRN_BITMAGIC_H
#define HIPERF_LRN_BITMAGIC_H

#include <utility>
#include <cstdint>

//
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain.
// See http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp
// MurmurHash3_x64_128
//
static inline uint64_t rotate_left(uint64_t val, uint16_t distance) {
  return (val << distance) | (val >> (64 - distance));
}

static inline uint64_t fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccduLL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53uLL;
  k ^= k >> 33;
  return k;
}

static inline std::pair<uint64_t, uint64_t> murmur3_128(uint64_t val, uint64_t seed) {
  uint64_t h1 = seed;
  uint64_t h2 = seed;

  uint64_t c1 = 0x87c37b91114253d5ull;
  uint64_t c2 = 0x4cf5ad432745937full;

  int length = 8;
  uint64_t k1 = 0;

  k1 = val;
  k1 *= c1;
  k1 = rotate_left(k1, 31);
  k1 *= c2;
  h1 ^= k1;

  h1 ^= length;
  h2 ^= length;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  h2 += h1;

  return std::make_pair(h1, h2);
}

static inline uint64_t murmur3_64(uint64_t val, uint64_t seed) {
  return murmur3_128(val, seed).first;
}

// fastmod library
// credits to Daniel Lemire: https://github.com/lemire/fastmod
static inline uint64_t fastmod_computeM_u32(uint32_t d) {
    return UINT64_C(0xFFFFFFFFFFFFFFFF) / d + 1;
}

static inline __uint128_t fastmod_computeM_u64(uint64_t d) {
    // what follows is just ((__uint128_t)0 - 1) / d) + 1 spelled out
    __uint128_t M = UINT64_C(0xFFFFFFFFFFFFFFFF);
    M <<= 64;
    M |= UINT64_C(0xFFFFFFFFFFFFFFFF);
    M /= d;
    M += 1;
    return M;
}

static inline uint64_t mul128_u32(uint64_t lowbits, uint32_t d) {
    return ((__uint128_t)lowbits * d) >> 64;
}

// This is for the 64-bit functions.
static inline uint64_t mul128_u64(__uint128_t lowbits, uint64_t d) {
    __uint128_t bottom_half = (lowbits & UINT64_C(0xFFFFFFFFFFFFFFFF)) * d;  // Won't overflow
    bottom_half >>= 64;  // Only need the top 64 bits, as we'll shift the lower half away;
    __uint128_t top_half = (lowbits >> 64) * d;
    __uint128_t both_halves = bottom_half + top_half;  // Both halves are already shifted down by 64
    both_halves >>= 64;                                // Get top half of both_halves
    return (uint64_t)both_halves;
}

static inline uint32_t fastdiv_u32(uint32_t a, uint64_t M) {
    return (uint32_t)(mul128_u32(M, a));
}

static inline uint64_t fastmod_u64(uint64_t a, __uint128_t M, uint64_t d) {
    __uint128_t lowbits = M * a;
    return mul128_u64(lowbits, d);
}

#endif // HIPERF_LRN_BITMAGIC_H
