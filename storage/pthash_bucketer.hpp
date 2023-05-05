#ifndef CALLFWD_PTHASH_BUCKETER_H_
#define CALLFWD_PTHASH_BUCKETER_H_

#include "bit_magic.hpp"

#include <folly/Likely.h>
#include <folly/lang/Hint.h>

struct SkewBucketer {
  // TODO: adjust to primes?
  explicit SkewBucketer(uint32_t dense_buckets, uint32_t sparse_buckets)
    : dense_buckets(dense_buckets)
    , sparse_buckets(sparse_buckets)
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
  {}

  explicit SkewBucketer(uint32_t num_buckets)
    : dense_buckets(0.3 * num_buckets)
    , sparse_buckets(num_buckets - dense_buckets)
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
  {}

  /* Returns an integer in range [-sparse_buckets .. dense_buckets) */
  uint32_t operator()(uint64_t hashed_key) const {
    static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
    if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
      return fastmod_u64(hashed_key, M_dense_buckets, dense_buckets);
    } else {
      return fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets)
        + dense_buckets;
    }
  }

  /* Equals to construction time parameter. */
  uint32_t num_buckets() const {
    return dense_buckets + sparse_buckets;
  }

  uint32_t dense_buckets;
  uint32_t sparse_buckets;
  __uint128_t M_dense_buckets;
  __uint128_t M_sparse_buckets;
};

#endif // CALLFWD_PTHASH_BUCKETER_H_
