#ifndef CALLFWD_PTHASH_PARTITIONER_INTERNAL_H_
#define CALLFWD_PTHASH_PARTITIONER_INTERNAL_H_

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/util/unreachable.h>

#include <memory>

namespace acero = arrow::acero;

/** Counts total number of written rows in metadata. */
struct CountingFileWriter final : arrow::dataset::FileWriter {
  explicit CountingFileWriter(std::shared_ptr<arrow::dataset::FileWriter> writer,
                              std::shared_ptr<arrow::KeyValueMetadata> metadata)
      : FileWriter(writer->schema(), writer->options(), NULLPTR, writer->destination())
      , writer(std::move(writer))
      , metadata(std::move(metadata))
  {}

  arrow::Status Write(const std::shared_ptr<arrow::RecordBatch>& batch) override {
    rows_written += batch->num_rows();
    return writer->Write(batch);
  }

  arrow::Future<> FinishInternal() override {
    arrow::Unreachable();
  }

  arrow::Future<> Finish() override {
    ARROW_RETURN_NOT_OK(
      metadata->Set("num_rows", std::to_string(rows_written)));
    return writer->Finish();
  }

  int64_t rows_written = 0;
  std::shared_ptr<arrow::dataset::FileWriter> writer;
  std::shared_ptr<arrow::KeyValueMetadata> metadata;
};

/** Arrow dataset format for CountingFileWriter. */
struct CountingIpcFileFormat : arrow::dataset::IpcFileFormat {
  arrow::Result<std::shared_ptr<arrow::dataset::FileWriter>> MakeWriter(
      std::shared_ptr<arrow::io::OutputStream> output,
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<arrow::dataset::FileWriteOptions> options,
      arrow::fs::FileLocator file_locator) const override;
};

using AsyncExecBatch = arrow::Future<std::optional<arrow::compute::ExecBatch>>;

struct PtHashPartitionerPriv {
  AsyncExecBatch GenExecBatch() const;
  arrow::MemoryPool* memory_pool;
  arrow::compute::ExecContext* exec_context;
  arrow::io::IOContext* io_context;
  std::shared_ptr<acero::ExecPlan> exec_plan;
  std::shared_ptr<arrow::io::MemoryMappedFile> source_file;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
  arrow::AsyncGenerator<std::shared_ptr<arrow::RecordBatch>> batch_generator;
  arrow::dataset::FileSystemDatasetWriteOptions write_options;
};

#endif // CALLFWD_PTHASH_PARTITIONER_INTERNAL_H_
