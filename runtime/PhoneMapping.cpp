#include "PhoneMapping.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <vector>
#include <glog/logging.h>
#include <folly/json.h>
#include <folly/Likely.h>
#include <folly/String.h>
#include <folly/Conv.h>
#include <folly/small_vector.h>
#include <folly/synchronization/Hazptr.h>

struct PnPartition {
  // Read pn => write pn_handle
  void Lookup(PnResult &inout);
  // Read pn_handle => check (pn < 0) or write (>= 0) pn, dno, dnc, xtra_handle
  void AccessPnBits(PnResult &inout);
  // Read pn_handle => set rn, set rn_handle
  void AccessRnBits(PnResult &inout);

  std::shared_ptr<arrow::Table> table;

  LocalBucketer local_bucketer;
  __int128_t M_table_size;
};

class Dataset : public folly::hazptr_obj_base<Dataset> {
 public:
  ~Dataset() noexcept;

  uint64_t hash_seed;
  std::vector<uint64_t> hashed_pilot;
  std::shared_ptr<arrow::UInt64Array> pilot;

  // chunked?
  SkewBucketer bucketer{num_buckets};
  // -> global bucket
  BucketPartitioner partitioner{bucketer, num_partitions};
  // -> partition (mod)
  std::vector<PnPartition> partition;
};

Dataset::~Dataset() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
  // munlock
}

void QueryEngine::QueryPN(uint64_t pn, PnResult &row) const {
  return QueryPNs(1, &pn, &row);
}

void QueryEngine::QueryPNs(size_t N, const uint64_t *pn, PnResult *rows) const {
  // TODO: try prefetch-unrolling
  for (size_t i = 0; i < N; ++i) {
    std::pair<uint64_t, uint64_t> h128 = murmur3_128(pn[i], hash_seed);
    uint32_t p = (h128.first ^ h128.second) & 0xF;
    uint32_t b = bucketer[p](h128.first);
    uint64_t h = h128.second ^ hashed_pilot[pilot[p][b]];
    rows[i] = fastmod_u64(h, M[p], table_size[p]);
  }
}

std::unique_ptr<Dataset> DatasetLoader::Open(const char *path) {
}

void DatasetLoader::Commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t rn_count = data->rnIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " RNs=" << rn_count;
}

DatasetLoader::DatasetLoader(std::unique_ptr<Data> data) {
  holder_.reset_protection(data.get());
  data->retire();
  data_ = data.release();
}

DatasetLoader::DatasetLoader(std::atomic<Data*> &global)
  : data_(holder_.protect(global))
{
}

void DatasetLoader::LogMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

uint64_t PhoneNumber::fromString(folly::StringPiece s) {
  std::string digits;

  // Remove punctuation
  s = folly::trimWhitespace(s);
  if (s.size() > 30) // Don't waste time on garbage
    return NONE;

  digits.reserve(s.size());
  std::copy_if(s.begin(), s.end(), std::back_inserter(digits),
               [](char c) { return strchr(" -()", c) == NULL; });
  s = digits;

  if (s.size() == 12) {
    s.removePrefix("+1");
  } else if (s.size() == 11) {
    s.removePrefix("1");
  }

  if (s.size() != 10)
    return NONE;

  if (auto asInt = folly::tryTo<uint64_t>(s))
    return asInt.value();
  return NONE;
}
