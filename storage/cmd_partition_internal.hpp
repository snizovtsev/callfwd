#ifndef CALLFWD_CMD_PARTITION_INTERNAL_H_
#define CALLFWD_CMD_PARTITION_INTERNAL_H_

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>

#include <condition_variable>
#include <memory>

struct PartitionSink {
  std::mutex mutex;
  int64_t num_rows = 0;
  std::string file_path;
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::KeyValueMetadata> metadata;
  std::shared_ptr<arrow::io::FileOutputStream> ostream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
};

struct PartitionTask {
  arrow::Status Init(const struct CmdPartitionPriv* cmd);
  arrow::Status HashKeys();
  arrow::Status RunColumn(int c);
  arrow::Result<arrow::RecordBatchVector> Run();
  arrow::Result<arrow::RecordBatchVector> operator()() { return Run(); }

  const struct CmdPartitionOptions* options;
  arrow::RecordBatchVector slice; // a slice of P batches
  std::vector<std::unique_ptr<arrow::RecordBatchBuilder>> output;
};

struct CmdPartitionPriv {
  arrow::Result<arrow::RecordBatchVector> ReadSlice(int64_t batch_index);
  arrow::Status Throttle();
  void ExecTask(PartitionTask task);
  void IoTask(arrow::RecordBatchVector records);

  const struct CmdPartitionOptions* options;
  arrow::compute::ExecContext* exec_context;
  const arrow::io::IOContext* io_context;

  std::shared_ptr<arrow::io::MemoryMappedFile> source_file;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> batch_reader;
  std::vector<PartitionSink> output;

  mutable std::mutex scheduler_mutex;
  mutable std::condition_variable producer_cv;
  arrow::Status last_status;
  int compute_semaphore = 0;
  int write_semaphore = 0;
};

#endif // CALLFWD_CMD_PARTITION_INTERNAL_H_
