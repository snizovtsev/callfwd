#ifndef CALLFWD_CMD_PTHASH_INTERNAL_H_
#define CALLFWD_CMD_PTHASH_INTERNAL_H_

#include "pthash_bucketer.hpp"
#include <arrow/api.h>
#include <arrow/ipc/api.h>

struct CmdPtHashPriv {
  arrow::Result<arrow::UInt8Array> BucketHistrogram(
    const arrow::ArrayVector& hash_chunks);
  arrow::Status SortBuckets(const uint8_t* load_factor);
  arrow::Status GroupHashes(const arrow::ListArray& bucket_bins);
  arrow::Status SelectPilots(const arrow::ListArray& bucket_bins,
                             const arrow::ListArray& hash_bins);
  arrow::Status MakeIndices(const arrow::ArrayVector& bucket_hash_chunks,
                            const arrow::ArrayVector& table_hash_chunks);
  arrow::Status WritePtHash();
  arrow::Status WriteData();

  arrow::MemoryPool* memory_pool;
  const struct CmdPtHashOptions* options;

  std::shared_ptr<const arrow::KeyValueMetadata> metadata;
  std::shared_ptr<arrow::Table> table;

  uint64_t hash_seed;
  uint64_t table_size;
  uint64_t rows_per_batch;
  uint32_t num_buckets;
  int chunk_bits;
  SkewBucketer bucketer;
  __uint128_t M_table_size;
  std::vector<uint64_t> hashed_pilot;

  // bucket numbers, grouped by size. |<nbuckets|
  std::shared_ptr<arrow::ListArray> bucket_bins;
  // table hash, grouped by bucket. |nkeys|
  std::shared_ptr<arrow::ListArray> hash_bins;
  // pilot numbers, |nbuckets|
  std::shared_ptr<arrow::UInt16Array> pilot_array;
  // reordering indices, |nkeys|
  std::shared_ptr<arrow::UInt32Array> indices;
};

#endif // CALLFWD_CMD_PTHASH_INTERNAL_H_
