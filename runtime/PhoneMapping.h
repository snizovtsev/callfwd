#ifndef CALLFWD_PHONEMAPPING_H
#define CALLFWD_PHONEMAPPING_H

#include <cstdint>
#include <cstddef>
#include <memory>
#include <limits>
#include <atomic>
#include <istream>

#include <folly/Range.h>
#include <folly/synchronization/HazptrHolder.h>

class PhoneNumber {
public:
  static constexpr uint64_t NONE =
    std::numeric_limits<uint64_t>::max();
  static uint64_t fromString(folly::StringPiece s);
};

class Dataset; /* opaque */

class DatasetLoader {
  /** Scan the directory, mmap files and do a sanity checks. */
  static std::unique_ptr<Dataset> Open(const char *path);

  /** Lock memory and commit pointer to a global. */
  void Commit(std::atomic<Dataset*> &global);

  /** Underlying structure. */
  std::unique_ptr<Dataset> dataset;
};

struct PnResult {
  int64_t pn;
  int64_t pn_handle;
  int64_t rn;
  int64_t rn_handle;
  int64_t xtra_handle;
  int32_t dno;
  bool    dnc;
};

ststruct RnSetResult {
  std::vector<uint64_t> pn_set;
  int64_t rn_handle; // ordered jump list
};

class QueryEngine {
 public:
  /** Construct taking ownership of Data. Used for tests. */
  QueryEngine(std::unique_ptr<Dataset> dataset);
  /** Construct from globals and hold protected reference. */
  QueryEngine(std::atomic<Dataset*> &global);
  /** Ensure move constructor exists */
  QueryEngine(QueryEngine&& rhs) noexcept;
  /** Default destructor. */
  ~QueryEngine() noexcept;
  /** Get default instance from global variable. */
  static QueryEngine Get() noexcept;
  /** Check if database is loaded. */
  static bool IsAvailable() noexcept;
  /** Log metadata to system journal. */
  void LogMetadata();

  int64_t num_pn_rows() const noexcept;
  int64_t num_rn_rows() const noexcept;
  int64_t num_xtra_rows() const noexcept;

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  //uint64_t getRN(uint64_t pn) const;
  void QueryPN(uint64_t pn, PnResult &row) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling queryRN() multiple times. */
  //void getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const;
  void QueryPNs(size_t N, const uint64_t *pn, PnResult *rows) const;

  /** Select rows by routing number prefix.
    * Use cursor methods to retrieve relevent rows. */
  //bool queryRNSet(uint64_t rn_handle, PnSetResult &row);
  // XXX: postpone second stage

 private:
  folly::hazptr_holder<> holder_;
  const Dataset *data_;
};

#endif // CALLFWD_PHONEMAPPING_H
