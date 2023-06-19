#include "cmd_pthash_internal.hpp"
#include "cmd_pthash.hpp"
#include "lrn_schema.hpp"

#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/bit_util.h>
#include <arrow/stl_iterator.h>
#include <cstdint>
#include <folly/Conv.h>
#include <glog/logging.h>
#include <boost/program_options/options_description.hpp>
#include <iostream>
#include <memory>

namespace bit_util = arrow::bit_util;
namespace po = boost::program_options;
using Status = arrow::Status;
template <class T> using Result = arrow::Result<T>;

std::unique_ptr<CmdPtHash> CmdPtHash::Make(arrow::MemoryPool* memory_pool)
{
  auto cmd = std::make_unique<CmdPtHash>();
  cmd->memory_pool = memory_pool ?: arrow::default_memory_pool();
  return cmd;
}

static Result<std::shared_ptr<arrow::Table>> ReadTable(
    arrow::ipc::RecordBatchFileReader* reader)
{
  arrow::RecordBatchVector batches;

  batches.resize(reader->num_record_batches());
  for (size_t i = 0; i < batches.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(batches[i], reader->ReadRecordBatch(i));
  }

  return arrow::Table::FromRecordBatches(reader->schema(), batches);
}

Status CmdPtHash::Init(const Options& options_) {
  options = &options_;
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::MemoryMappedFile
                        ::Open(options->partition_path, arrow::io::FileMode::READ));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader
                        ::Open(file, arrow::ipc::IpcReadOptions{}));

  metadata = reader->metadata()->Copy();
  ARROW_ASSIGN_OR_RAISE(table, ReadTable(std::move(reader).get()));

  int64_t num_rows = table->num_rows();
  table_size = table->num_rows() / options->tweak_alpha;
  num_buckets = std::ceil(options->tweak_c * num_rows / std::log2(num_rows));
  bucketer = SkewBucketer{num_buckets};
  hash_seed = folly::to<uint64_t>(metadata->Get("hash_seed").ValueOr("absent"));

  return Status::OK();
}

Result<arrow::UInt8Array>
CmdPtHashPriv::BucketHistrogram(const arrow::ArrayVector& hash_chunks) {
  arrow::TypedBufferBuilder<uint8_t> load_factor_builder(memory_pool);
  ARROW_RETURN_NOT_OK(load_factor_builder.Append(num_buckets, 0));
  auto load_factor = load_factor_builder.mutable_data();

  for (const auto& chunk : hash_chunks) {
    const auto& array = dynamic_cast<arrow::UInt64Array&>(*chunk);
    const int64_t num_rows = array.length();
    for (int64_t i = 0; i < num_rows; ++i) {
      const uint32_t bucket = bucketer(array.Value(i));
      DCHECK_LT(load_factor[bucket], UINT8_MAX);
      load_factor[bucket]++;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto load_factor_data, load_factor_builder.Finish());
  return {arrow::UInt8Array(num_buckets, std::move(load_factor_data))};
}

Status CmdPtHashPriv::SortBuckets(const uint8_t* load_factor) {
  auto indices = std::make_shared<arrow::UInt32Builder>(memory_pool);
  arrow::ListBuilder bins_builder(memory_pool, indices);
  const uint32_t max_freq = *std::max_element(
      load_factor, load_factor + num_buckets);

  LOG(INFO) << "buckets: " << num_buckets << ", max: " << max_freq;

  ARROW_RETURN_NOT_OK(indices->Reserve(num_buckets));
  ARROW_RETURN_NOT_OK(bins_builder.Reserve(max_freq));

  for (uint32_t freq = 1; freq <= max_freq; ++freq) {
    ARROW_RETURN_NOT_OK(bins_builder.Append());
    for (uint32_t bucket = 0; bucket < num_buckets; ++bucket) {
      if (LIKELY(load_factor[bucket] != freq))
        continue;
      indices->UnsafeAppend(bucket);
    }
  }

  return bins_builder.Finish(&buckets);
}

Status CmdPtHashPriv::GroupHashes(const arrow::ListArray& buckets) {
  const auto hash1_column = table->GetColumnByName("hash1");
  const auto hash2_column = table->GetColumnByName("hash2");
  const int64_t num_keys = table->num_rows();
  const int num_chunks = hash1_column->num_chunks();
  CHECK_EQ(hash2_column->num_chunks(), num_chunks);

  arrow::TypedBufferBuilder<uint64_t> hash_builder(memory_pool);
  arrow::TypedBufferBuilder<int32_t> offset_builder(memory_pool);
  ARROW_RETURN_NOT_OK(hash_builder.Append(num_keys,  0));
  ARROW_RETURN_NOT_OK(offset_builder.Append(num_buckets,  0));

  int32_t *offset = offset_builder.mutable_data();

  int32_t key_index = 0;
  for (int64_t freq = 1; freq <= buckets.length(); ++freq) {
    const auto& bin = dynamic_cast<arrow::UInt32Array&>(
        *buckets.value_slice(freq - 1));

    for (int64_t j = 0; j < bin.length(); ++j) {
      const uint32_t bucket = bin.Value(j);
      key_index += freq;
      offset[bucket] = key_index;
    }
  }
  CHECK_EQ(key_index, num_keys);

  ARROW_RETURN_NOT_OK(origin.Append(num_keys, 0));
  uint32_t *origin_data = origin.mutable_data();
  uint64_t *hash_data = hash_builder.mutable_data();

  key_index = 0;
  for (int chunk = 0; chunk < num_chunks; ++chunk) {
    const auto& bucket_hash = dynamic_cast<arrow::UInt64Array&>(
        *hash1_column->chunk(chunk));
    const auto& table_hash = dynamic_cast<arrow::UInt64Array&>(
        *hash2_column->chunk(chunk));
    const int64_t chunk_rows = bucket_hash.length();
    CHECK_EQ(table_hash.length(), bucket_hash.length());

    for (int64_t i = 0; i < chunk_rows; ++i) {
      const uint32_t bucket = bucketer(bucket_hash.Value(i));
      const int32_t pos = --offset[bucket];
      DCHECK_GE(pos, 0);
      hash_data[pos] = table_hash.Value(i);
      origin_data[pos] = key_index + i;
    }
    key_index += chunk_rows;
  }
  CHECK_EQ(key_index, num_keys);

  uint32_t hash_bin = 0;
  key_index = 0;
  for (int64_t freq = 1; freq <= buckets.length(); ++freq) {
    const auto& bin = dynamic_cast<arrow::UInt32Array&>(
        *buckets.value_slice(freq - 1));

    for (int64_t j = bin.length(); j > 0; --j) {
      offset[hash_bin++] = key_index;
      key_index += freq;
    }
  }
  CHECK_EQ(key_index, num_keys);

  ARROW_ASSIGN_OR_RAISE(auto offset_buffer, offset_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(auto hash_buffer, hash_builder.Finish());

  return arrow::ListArray
    ::FromArrays(arrow::Int32Array(hash_bin, std::move(offset_buffer)),
                 arrow::UInt64Array(num_keys, std::move(hash_buffer)),
                 memory_pool)
    .Value(&hashes);
}

Status CmdPtHashPriv::SelectPilots(const arrow::ListArray& hash_bins) {
  uint32_t oldreport = table->num_rows();
  uint64_t pilot_max = 0;

  uint32_t pilot_limit = UINT16_MAX - 10;
  std::vector<uint64_t> hashed_pilot(pilot_limit);
  for (uint32_t p = 0; p < pilot_limit; ++p)
    hashed_pilot[p] = murmur3_64(p, hash_seed);

  const uint32_t* bucket_desc_data = buckets->values()->data()->GetValues<uint32_t>(1);
  const uint64_t* hash_data = hash_bins.values()->data()->GetValues<uint64_t>(1);
  uint32_t* origin_data = origin.mutable_data();

  ARROW_RETURN_NOT_OK(take_indices.Append(table_size, 0));
  uint32_t *indices_data = take_indices.mutable_data();

  taken.Reset();
  ARROW_RETURN_NOT_OK(taken.Append(table_size, false));
  uint8_t *taken_bits = taken.mutable_data();

  ARROW_RETURN_NOT_OK(pilot.Append(num_buckets, 0));
  uint16_t *pilot_data = pilot.mutable_data();

  __uint128_t M = fastmod_computeM_u64(table_size);
  for (int64_t bi = hash_bins.length(); bi > 0; --bi) {
    uint32_t lb = bucket_desc_data[bi];
    int64_t row_start = hash_bins.value_offset(bi - 1);
    int64_t row_end = hash_bins.value_offset(bi);
    int64_t row_idx = row_end - 1;
    bool retry = true;

    for (uint32_t trial = 0; retry && trial < pilot_limit; ++trial) {
      for (retry = false; row_idx >= row_start; --row_idx) {
        uint64_t h = hash_data[row_idx] ^ hashed_pilot[trial];
        uint32_t p = fastmod_u64(h, M, table_size);
        if (!bit_util::GetBit(taken_bits, p)) {
          indices_data[p] = origin_data[row_idx];
          bit_util::SetBit(taken_bits, p);
          origin_data[row_idx] = p;
        } else {
          retry = true;
          break;
        }
      }

      if (retry) {
        for (row_idx += 1; row_idx < row_end; ++row_idx) {
          uint32_t p = origin_data[row_idx];
          origin_data[row_idx] = indices_data[p];
          indices_data[p] = 0;
          bit_util::ClearBit(taken_bits, p);
        }
        --row_idx;
      } else {
        pilot_data[lb] = trial;
        if (trial > pilot_max)
          pilot_max = trial;
      }
    }

    CHECK(!retry);

    if (oldreport - row_idx > table->num_rows() / 100) {
      LOG(INFO) << lb << ": " << pilot_data[lb]
                << " maxp " << pilot_max
                << " bs " << row_end - row_start
                << " off " << row_start;
      oldreport = row_idx;
    }
  }
  LOG(INFO) << "maxp " << pilot_max;

  size_t show_empty = 100;
  std::cout << "!taken: ";
  for (int64_t i = 0; show_empty && i < taken.length(); ++i) {
    if (bit_util::GetBit(taken_bits, i))
      continue;
    std::cout << i << ' ';
    --show_empty;
  }
  std::cout << "..." << std::endl;

  return Status::OK();
}

Status CmdPtHash::Run(int64_t limit) {
  /*
   * in: bucketer
   * in: hash1
   * out: bucket_size
   */
  LOG(INFO) << "Sizing buckets...";
  auto hash1_column = table->GetColumnByName("hash1");
  ARROW_ASSIGN_OR_RAISE(
    auto load_factor, BucketHistrogram(hash1_column->chunks()));

  /*
   * in: bucket_size
   * out: buckets
   */
  LOG(INFO) << "Sorting buckets...";
  ARROW_RETURN_NOT_OK(SortBuckets(std::move(load_factor).raw_values()));

  LOG(INFO) << "Group hashes...";
  ARROW_RETURN_NOT_OK(GroupHashes(*buckets));

  LOG(INFO) << "Select pilots...";
  ARROW_RETURN_NOT_OK(SelectPilots(*hashes));

#if 0
  auto indices = arrow::UInt32Array(
    table_size,
    take_indices.FinishWithLength(table_size).ValueOrDie(),
    taken.Finish().ValueOrDie());
  std::cout << indices.ToString() << std::endl;

  LOG(INFO) << "Take...";
  std::vector<int> select_columns;
  select_columns.reserve(table->num_columns() - 2);
  for (int c = 0; c < table->num_columns(); ++c) {
    const std::string& name = table->field(c)->name();
    if (name != "hash1" && name != "hash2")
      select_columns.push_back(c);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto table_without_hash, table->SelectColumns(select_columns));

  res = arrow::compute::Take(table, indices)->table();
  std::cout << res->ToString();

  LOG(INFO) << "Verify...";
  table_index = 0;
  for (auto& chunk : res->GetColumnByName("pn_bits")->chunks()) {
    auto pn_bits = chunk->data()->GetValues<uint8_t>(0, 0);
    auto pn_data = chunk->data()->GetValues<uint64_t>(1, 0);
    for (int64_t i = 0; i < chunk->length(); ++i, ++table_index) {
      if (UNLIKELY(!bit_util::GetBit(pn_bits, i)))
        continue;
      uint64_t pn = pn_data[i] >> LRN_BITS_PN_SHIFT;
      auto h = murmur3_128(pn, hash_seed);
      uint32_t b = bucketer(h.first);
      uint64_t th = h.second ^ hashed_pilot[pilot_data[b]];
      uint32_t p = fastmod_u64(th, M, table_size);
      LOG_IF(ERROR, p != table_index) << table_index << ": pn=" << pn << " " << p;
    }
  }
#endif

  return Status::OK();
}

Status CmdPtHash::Finish(bool incomplete) {
#if 0
  ARROW_ASSIGN_OR_RAISE(auto pthash_buf, pilot.FinishWithLength(num_buckets));
  auto pthash = std::make_shared<arrow::UInt16Array>(num_buckets, pthash_buf);
  LOG(INFO) << "pilot bytes: " << pthash_buf->size() << " ("
    << num_buckets << " buckets)";
  LOG(INFO) << pthash->ToString();

  auto pthash_table = arrow::Table::Make(
    arrow::schema({arrow::field("pilot", pthash->type())}), {pthash});

  ARROW_ASSIGN_OR_RAISE(auto ostream, arrow::io::FileOutputStream::Open(
      options->data_output_path));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(
      ostream.get(), res->schema(),
      arrow::ipc::IpcWriteOptions{},
      metadata));
  ARROW_RETURN_NOT_OK(writer->WriteTable(*res));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto ostream_index, arrow::io::FileOutputStream
                        ::Open(options->data_output_path));
  ARROW_ASSIGN_OR_RAISE(auto writer_index, arrow::ipc::MakeFileWriter(
      ostream_index.get(), pthash_table->schema(),
      arrow::ipc::IpcWriteOptions{}));
  ARROW_RETURN_NOT_OK(writer_index->WriteTable(*pthash_table));
  ARROW_RETURN_NOT_OK(writer_index->Close());
#endif

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

const CmdDescription CmdPtHash::description = {
  .name = "pthash",
  .args = "partition_path...",
  .abstract = "Build a PtHash indexed dataset",
  .help = ""
};

template <>
void CmdOps<CmdPtHash>::BindOptions(po::options_description& description,
                                    CmdPtHashOptions& options)
{
  description.add_options()
      ("output,o",
       po::value(&options.data_output_path)->required())
      ("index,i",
       po::value(&options.index_output_path)->required())
      ("tweak-c,c",
       po::value(&options.tweak_c)->default_value(5.0),
       "Bucket density coefficient")
      ("tweak-alpha,a",
       po::value(&options.tweak_alpha)->default_value(0.98),
       "Value domain density coefficient");
}

template <>
Status CmdOps<CmdPtHash>::StoreArgs(const po::variables_map& vm,
                                    const std::vector<std::string>& args,
                                    CmdPtHashOptions& options)
{
  if (args.size() != 1)
    return Status::Invalid("1 positional arg expected");
  options.partition_path = args[0];
  return Status::OK();
}
