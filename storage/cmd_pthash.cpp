#include "cmd_convert_internal.hpp"
#include "cmd_pthash_internal.hpp"
#include "cmd_pthash.hpp"
#include "lrn_schema.hpp"

#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visit_type_inline.h>
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
  M_table_size = fastmod_computeM_u64(table_size);
  num_buckets = std::ceil(options->tweak_c * num_rows / std::log2(num_rows));
  bucketer = SkewBucketer{num_buckets};
  hash_seed = folly::to<uint64_t>(metadata->Get("hash_seed").ValueOr("absent"));
  rows_per_batch = folly::to<uint64_t>(metadata->Get("rows_per_batch").ValueOr("absent"));

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

  return bins_builder.Finish(&bucket_bins);
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

  uint64_t *hash_data = hash_builder.mutable_data();
  int32_t *offset_data = offset_builder.mutable_data();
  int32_t key_index = 0;

  for (int64_t freq = 1; freq <= buckets.length(); ++freq) {
    const auto& bin = dynamic_cast<arrow::UInt32Array&>(
        *buckets.value_slice(freq - 1));

    for (int64_t j = 0; j < bin.length(); ++j) {
      const uint32_t bucket = bin.Value(j);
      key_index += freq;
      offset_data[bucket] = key_index;
    }
  }
  CHECK_EQ(key_index, num_keys);

  for (int chunk = 0; chunk < num_chunks; ++chunk) {
    const auto& bucket_hash = dynamic_cast<arrow::UInt64Array&>(
        *hash1_column->chunk(chunk));
    const auto& table_hash = dynamic_cast<arrow::UInt64Array&>(
        *hash2_column->chunk(chunk));

    const int64_t chunk_rows = bucket_hash.length();
    CHECK_EQ(table_hash.length(), bucket_hash.length());

    for (int64_t i = 0; i < chunk_rows; ++i) {
      const uint32_t bucket = bucketer(bucket_hash.Value(i));
      const int32_t pos = --offset_data[bucket];
      DCHECK_GE(pos, 0);
      hash_data[pos] = table_hash.Value(i);
    }
  }

  uint32_t hash_bin = 0;
  key_index = 0;
  for (int64_t freq = 1; freq <= buckets.length(); ++freq) {
    int64_t rep = buckets.value_length(freq - 1);
    for (; rep; --rep) {
      offset_data[hash_bin++] = key_index;
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
    .Value(&hash_bins);
}

Status CmdPtHashPriv::SelectPilots(const arrow::ListArray& bucket_bins,
                                   const arrow::ListArray& hash_bins)
{
  uint64_t pilot_max = 0;
  uint32_t pilot_limit = UINT16_MAX - 10;
  uint32_t oldreport = table->num_rows();

  arrow::TypedBufferBuilder<bool> taken(memory_pool);
  arrow::TypedBufferBuilder<uint16_t> pilot_builder(memory_pool);
  std::vector<uint32_t> positions(bucket_bins.length());

  hashed_pilot.resize(pilot_limit);
  for (uint32_t p = 0; p < pilot_limit; ++p)
    hashed_pilot[p] = murmur3_64(p, hash_seed);

  const auto& ordered_buckets = dynamic_cast<arrow::UInt32Array&>(
      *bucket_bins.values());
  const auto& hash_values = dynamic_cast<arrow::UInt64Array&>(
      *hash_bins.values());

  ARROW_RETURN_NOT_OK(taken.Append(table_size, false));
  uint8_t *taken_bits = taken.mutable_data();

  ARROW_RETURN_NOT_OK(pilot_builder.Append(num_buckets, 0));
  uint16_t *pilot_data = pilot_builder.mutable_data();

  for (int64_t bi = hash_bins.length(); bi > 0; --bi) {
    uint32_t bucket = ordered_buckets.Value(bi);
    int64_t row_start = hash_bins.value_offset(bi - 1);
    int64_t row_end = hash_bins.value_offset(bi);
    int64_t row_idx = row_end - 1;
    bool retry = true;

    for (uint32_t trial = 0; retry && trial < pilot_limit; ++trial) {
      positions.clear();

      for (retry = false; row_idx >= row_start; --row_idx) {
        uint64_t h = hash_values.Value(row_idx) ^ hashed_pilot[trial];
        uint32_t p = fastmod_u64(h, M_table_size, table_size);
        if (!bit_util::GetBit(taken_bits, p)) {
          bit_util::SetBit(taken_bits, p);
          positions.push_back(p);
        } else {
          retry = true;
          break;
        }
      }

      if (retry) {
        for (uint32_t p : positions)
          bit_util::ClearBit(taken_bits, p);
        row_idx = row_end - 1;
      } else {
        pilot_data[bucket] = trial;
        if (trial > pilot_max)
          pilot_max = trial;
      }
    }

    CHECK(!retry);

    if (oldreport - row_idx > table->num_rows() / 100) {
      LOG(INFO) << bucket << ": " << pilot_data[bucket]
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

  ARROW_ASSIGN_OR_RAISE(auto pilot_buf, pilot_builder.Finish());
  pilot_array = std::make_shared<arrow::UInt16Array>(
      num_buckets, std::move(pilot_buf));
  return Status::OK();
}

Status CmdPtHashPriv::MakeIndices(const arrow::ArrayVector& bucket_hash_chunks,
                                  const arrow::ArrayVector& table_hash_chunks)
{
  arrow::UInt32Builder indices_builder(memory_pool);
  ARROW_RETURN_NOT_OK(indices_builder.AppendEmptyValues(table_size));

  const size_t num_chunks = bucket_hash_chunks.size();
  CHECK_EQ(num_chunks, table_hash_chunks.size());

  chunk_bits = 0;
  for (const auto& chunk : bucket_hash_chunks) {
    const int64_t max_index = chunk->length() - 1;
    chunk_bits = std::max(bit_util::NumRequiredBits(max_index),
                          chunk_bits);
  }

  if (chunk_bits + bit_util::NumRequiredBits(num_chunks) > 32)
    return Status::Invalid("Cannot pack row coordinates into 32 bits");

  for (size_t c = 0; c < num_chunks; ++c) {
    const auto& bucket_hash = dynamic_cast<arrow::UInt64Array&>(
        *bucket_hash_chunks[c]);
    const auto& table_hash = dynamic_cast<arrow::UInt64Array&>(
        *table_hash_chunks[c]);
    const int64_t chunk_rows = bucket_hash.length();
    CHECK_EQ(table_hash.length(), bucket_hash.length());
    const uint32_t index_high = (c + 1) << chunk_bits;

    for (int64_t r = 0; r < chunk_rows; ++r) {
      const uint32_t bucket = bucketer(bucket_hash.Value(r));
      const auto pilot = pilot_array->Value(bucket);
      const uint64_t h = table_hash.Value(r) ^ hashed_pilot[pilot];
      const uint32_t p = fastmod_u64(h, M_table_size, table_size);
      indices_builder[p] = r | index_high;
    }
  }

  return indices_builder.Finish(&indices);
}

Status CmdPtHash::Run(int64_t limit) {
  auto& hash1_chunks = table->GetColumnByName("hash1")->chunks();
  auto& hash2_chunks = table->GetColumnByName("hash2")->chunks();

  LOG(INFO) << "Sizing buckets...";
  ARROW_ASSIGN_OR_RAISE(
      auto load_factor, BucketHistrogram(hash1_chunks));

  LOG(INFO) << "Sorting buckets...";
  ARROW_RETURN_NOT_OK(
      SortBuckets(std::move(load_factor).raw_values()));

  LOG(INFO) << "Group hashes...";
  ARROW_RETURN_NOT_OK(
      GroupHashes(*bucket_bins));

  LOG(INFO) << "Select pilots...";
  ARROW_RETURN_NOT_OK(
      SelectPilots(*std::move(bucket_bins),
                   *std::move(hash_bins)));

  LOG(INFO) << "Make indices...";
  ARROW_RETURN_NOT_OK(
      MakeIndices(hash1_chunks, hash2_chunks));

  return Status::OK();
}

Status CmdPtHashPriv::WritePtHash() {
  auto table = arrow::Table::Make(
    arrow::schema({arrow::field("pthash_pilot", pilot_array->type())}),
    {pilot_array});

  ARROW_ASSIGN_OR_RAISE(auto ostream, arrow::io::FileOutputStream
                        ::Open(options->index_output_path));

  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(
      ostream.get(), table->schema(),
      arrow::ipc::IpcWriteOptions{
        .memory_pool = memory_pool,
      }, metadata));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  return writer->Close();
}

struct TakeAC {
  template <class T>
  static constexpr bool is_simple_type =
    arrow::has_c_type<T>::value ||
    arrow::is_decimal_type<T>::value ||
    arrow::is_fixed_size_binary_type<T>::value;

  template <typename data_t, typename indices_t>
  std::enable_if_t<is_simple_type<data_t>, Status>
  Visit(const data_t& type, const indices_t* indices, int64_t length) {
    using BuilderType = typename arrow::TypeTraits<data_t>::BuilderType;
    using ArrayType = typename arrow::TypeTraits<data_t>::ArrayType;
    auto builder = static_cast<BuilderType*>(target);
    const indices_t row_mask = (1 << chunk_bits) - 1;

    ARROW_RETURN_NOT_OK(builder->Reserve(length));

    for (int64_t i = 0; i < length; ++i) {
      const int chunk_index = indices[i] >> chunk_bits;
      const int64_t row_index = indices[i] & row_mask;

      if (LIKELY(chunk_index)) {
        const auto& chunk = static_cast<ArrayType&>(
            *source->chunk(chunk_index - 1));
        builder->UnsafeAppend(chunk.GetView(row_index));
      } else {
        ARROW_RETURN_NOT_OK(builder->AppendEmptyValue());
      }
    }

    return Status::OK();
  }

  template <typename data_t, typename indices_t>
  std::enable_if_t<!is_simple_type<data_t>, Status>
  Visit(const data_t& type, const indices_t* indices, int64_t length) {
    return Status::NotImplemented("not a simple type");
  }

  const arrow::ChunkedArray* source;
  arrow::ArrayBuilder* target;
  int chunk_bits;
};

static Result<std::shared_ptr<arrow::Table>>
ExcludeHashColumns(const arrow::Table& table) {
  std::vector<int> select_columns;

  select_columns.reserve(table.num_columns() - 2);
  for (int c = 0; c < table.num_columns(); ++c) {
    const std::string& name = table.field(c)->name();
    if (name != "hash1" && name != "hash2")
      select_columns.push_back(c);
  }

  return table.SelectColumns(select_columns);
}

Status CmdPtHashPriv::WriteData() {
  RegularTableWriter writer;

  LOG(INFO) << "Take...";

  ARROW_ASSIGN_OR_RAISE(
      auto table_without_hash, ExcludeHashColumns(*table));

  ARROW_RETURN_NOT_OK(
      writer.Reset(table_without_hash->schema(),
                   options->data_output_path,
                   memory_pool, rows_per_batch));

  const uint32_t* ind = indices->raw_values();
  const uint32_t* end = indices->raw_values() + indices->length();

  for (; ind < end; ind += writer.rows_per_batch()) {
    int64_t length = writer.rows_per_batch();

    if (ind + length > end)
      length = end - ind;

    for (int c = 0; c < table_without_hash->num_columns(); ++c) {
      auto column = table_without_hash->column(c);
      TakeAC take;
      take.source = column.get();
      take.chunk_bits = chunk_bits;
      take.target = writer.builder->GetField(c);
      ARROW_RETURN_NOT_OK(
          arrow::VisitTypeInline(*column->type(), &take, ind, length));
    }

    ARROW_RETURN_NOT_OK(writer.Flush());
  }

  ARROW_RETURN_NOT_OK(writer.Finish());

  return Status::OK();
}

Status CmdPtHash::Finish(bool incomplete) {
  LOG(INFO) << "Writing...";
  ARROW_RETURN_NOT_OK(WritePtHash());
  if (!incomplete)
    ARROW_RETURN_NOT_OK(WriteData());
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
