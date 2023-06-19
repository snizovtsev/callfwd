#ifndef CALLFWD_CMD_PTHASH_INTERNAL_H_
#define CALLFWD_CMD_PTHASH_INTERNAL_H_

#include "pthash_bucketer.hpp"
#include <arrow/api.h>
#include <arrow/ipc/api.h>

struct CmdPtHashPriv {
  arrow::Result<arrow::UInt8Array>
  BucketHistrogram(const arrow::ArrayVector& hash_chunks);

  arrow::Status SortBuckets(const uint8_t* load_factor);
  arrow::Status GroupHashes(const arrow::ListArray& sorted_buckets);
  arrow::Status SelectPilots(const arrow::ListArray& hash_bins);

  arrow::MemoryPool* memory_pool;
  const struct CmdPtHashOptions* options;

  std::shared_ptr<const arrow::KeyValueMetadata> metadata;
  std::shared_ptr<arrow::Table> table;
  std::shared_ptr<arrow::Table> res;

  uint64_t hash_seed;
  uint64_t table_size;
  uint32_t num_buckets;
  SkewBucketer bucketer;

  // bucket numbers sorted by descending size
  std::shared_ptr<arrow::ListArray> buckets; // rename: bucket_bins
  // offset + hash => list<uint64> hash
  std::shared_ptr<arrow::ListArray> hashes; // rename: hash_bins

  // a row grouper (#non_zero_buckets)
  arrow::TypedBufferBuilder<uint32_t> origin;
  // pthash pilot numbers (#buckets)
  arrow::TypedBufferBuilder<uint16_t> pilot;
  // pthash result (#table)
  arrow::TypedBufferBuilder<uint32_t> take_indices;
  // validity bitmap
  arrow::TypedBufferBuilder<bool> taken;
};

#endif // CALLFWD_CMD_PTHASH_INTERNAL_H_
