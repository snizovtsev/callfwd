#ifndef CALLFWD_PN_ORDERED_JOIN_INTERNAL_H_
#define CALLFWD_PN_ORDERED_JOIN_INTERNAL_H_

#include "csv_reader.hpp"

#include <folly/GroupVarint.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <map>
#include <cstdint>

/*
 * Combines table builder, file stream and ipc writer into a single entity.
 * In addition to a generic IPC format it guarantee additional invariants:
 *  - All chunks except last one have the same number of rows,
 *  - `num_rows` footer metadata contains a total number of rows.
 */
struct RegularTableWriter {
  arrow::Status Reset(const std::shared_ptr<arrow::Schema>& schema,
                      const std::string& file_path,
                      arrow::MemoryPool* memory_pool,
                      int64_t rows_per_batch);
  arrow::Status Advance();
  arrow::Status Finish();

  int64_t rows_per_batch() const {
    return builder->GetField(0)->capacity();
  }

  int64_t num_record_batches() const {
    return writer->stats().num_record_batches;
  }

  template <typename T>
  T* GetFieldAs(int i) const {
    return builder->GetFieldAs<T>(i);
  }

  // write metadata
  int64_t header_bytes = 0;
  int64_t record_bytes = 0;
  int64_t tail_bytes = 0;
  int64_t footer_bytes = 0;
  int64_t last_report_bytes = 0;

  std::string file_path;
  std::shared_ptr<arrow::KeyValueMetadata> metadata;
  std::unique_ptr<arrow::RecordBatchBuilder> builder;
  std::shared_ptr<arrow::io::FileOutputStream> ostream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
};

struct StringAppender {
  std::string data;
  void operator()(folly::StringPiece sp) {
    data.append(sp.data(), sp.size());
  }
};

class MonotonicVarintSequenceEncoder {
 public:
  using Encoder = folly::GroupVarintEncoder<uint64_t, StringAppender>;

  void Add(uint64_t head) {
    CHECK_GE(head, prev_);
    encoder_.add(head - prev_);
    prev_ = head;
  }

  const std::string& Finish() {
    encoder_.finish();
    return encoder_.output().data;
  }

 private:
  Encoder encoder_{{}};
  uint64_t prev_ = 0;
};

struct PnOrderedJoinPriv {
  PnRecord row;
  PnMultiReader reader;
  PnRecordJoiner joiner;
  RegularTableWriter pn_writer;
  RegularTableWriter ym_writer;
  RegularTableWriter rn_writer;
  uint32_t youmail_row_index = 0;
  std::map<uint64_t, MonotonicVarintSequenceEncoder> rn_data;
};

#endif // CALLFWD_PN_ORDERED_JOIN_INTERNAL_H_
