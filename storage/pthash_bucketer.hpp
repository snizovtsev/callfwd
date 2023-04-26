#ifndef CALLFWD_PTHASH_BUCKETER_H_
#define CALLFWD_PTHASH_BUCKETER_H_

#include "bit_magic.hpp"

#include <folly/Likely.h>
#include <folly/lang/Hint.h>

struct SkewBucketer {
  // TODO: adjust to primes?
  explicit SkewBucketer(uint32_t num_buckets)
    : dense_buckets(0.3 * num_buckets)
    , sparse_buckets(num_buckets - dense_buckets)
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
  {}

  /* Returns an integer in range [-sparse_buckets .. dense_buckets) */
  int32_t operator()(uint64_t hashed_key) const {
    static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
    int32_t ret;
    if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
      ret = fastmod_u64(hashed_key, M_dense_buckets, dense_buckets);
      folly::compiler_may_unsafely_assume(ret >= 0);
    } else {
      ret = -1 - fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets);
      folly::compiler_may_unsafely_assume(ret < 0);
    }
    return ret;
  }

  /* Return an integer in range [0 .. num_buckets) */
  uint32_t to_index(int32_t bucket) const {
    return (bucket >= 0) ? bucket : (dense_buckets - bucket - 1);
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

template<class IntegerA, class IntegerB>
IntegerA DivideAndRoundUp(IntegerA a, IntegerB b) {
  return (a + b - 1) / b;
}

struct BucketPartitioner {
  explicit BucketPartitioner(const SkewBucketer& bucketer, uint16_t num_partitions)
    : BucketPartitioner(DivideAndRoundUp(bucketer.dense_buckets, num_partitions),
                        DivideAndRoundUp(bucketer.sparse_buckets, num_partitions))
  {}

  explicit BucketPartitioner(uint32_t dense_block, uint32_t sparse_block)
    : M_dense_block(fastmod_computeM_u32(dense_block))
    , M_sparse_block(fastmod_computeM_u32(sparse_block))
  {}

  /* Returns a number in range [0, num_partitions) */
  uint16_t operator()(int32_t bucket) const {
    if (bucket >= 0) {
      return fastdiv_u32(bucket, M_dense_block);
    } else {
      return fastdiv_u32(-(bucket + 1), M_sparse_block);
    }
  }

  uint64_t M_dense_block;
  uint64_t M_sparse_block;
};

struct LocalBucketer {
  explicit LocalBucketer(const SkewBucketer& bucketer, uint16_t num_partitions,
                         int32_t sample_bucket)
    : LocalBucketer(bucketer.dense_buckets, bucketer.sparse_buckets, num_partitions,
                    BucketPartitioner{bucketer, num_partitions}(sample_bucket))
  {}

  explicit LocalBucketer(uint32_t dense_buckets, uint32_t sparse_buckets,
                         uint16_t num_partitions, uint16_t partition)
    : dense_offset(DivideAndRoundUp(dense_buckets, num_partitions) * partition)
    , sparse_offset(
         DivideAndRoundUp(sparse_buckets, num_partitions) * partition
       - DivideAndRoundUp(dense_buckets, num_partitions)
      )
  {}

  /* Returns a number in range [0, DivideAndRoundUp(num_buckets, num_partitions)] */
  uint32_t operator()(int32_t bucket) const {
    if (bucket >= 0)
      return bucket - dense_offset;
    else
      return -(bucket + 1) - sparse_offset;
  }

  int32_t dense_offset;
  int32_t sparse_offset;
};

#endif // CALLFWD_PTHASH_BUCKETER_H_
