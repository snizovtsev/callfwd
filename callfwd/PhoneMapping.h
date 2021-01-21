#ifndef CALLFWD_PHONEMAPPING_H
#define CALLFWD_PHONEMAPPING_H

#include <cstdint>
#include <memory>
#include <limits>
#include <cstddef>
#include <atomic>
#include <istream>

#include <folly/Range.h>
#include <folly/synchronization/HazptrHolder.h>

namespace folly { struct dynamic; }

class PhoneNumber {
public:
  static constexpr uint64_t NONE =
    std::numeric_limits<uint64_t>::max();
  static uint64_t fromString(folly::StringPiece s);
};

class PhoneMapping {
 public:
  class Data; /* opaque */
  class Cursor; /* opaque */

  class Builder {
  public:
    Builder();
    ~Builder() noexcept;

    /** Attach arbitrary metadata. */
    void setMetadata(const folly::dynamic &meta);

    /** Preallocate memory for expected number of records. */
    void sizeHint(size_t numRecords);

    /** Add a new row into the scratch buffer.
      * Throws `runtime_error` if key already exists. */
    Builder& addRow(uint64_t pn, uint64_t rn);

    /** Add many rows from CSV text stream. */
    void fromCSV(std::istream &in, size_t& line, size_t limit);

    /** Build indexes and release the data. */
    PhoneMapping build();

    /** Build indexes and commit data to global. */
    void commit(std::atomic<Data*> &global);
  private:
    std::unique_ptr<Data> data_;
  };

  /** Construct taking ownership of Data. Used for tests. */
  PhoneMapping(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  PhoneMapping(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  PhoneMapping(PhoneMapping&& rhs) noexcept;
  /** Get default US instance from global variable. */
  static PhoneMapping getUS() noexcept;
  /** Get default CA instance from global variable. */
  static PhoneMapping getCA() noexcept;
  /** Check if DB fully loaded into memory. */
  static bool isAvailable() noexcept;
  ~PhoneMapping() noexcept;

  /** Get total number of records */
  size_t size() const noexcept;

  /** Log metadata to system journal */
  void printMetadata();

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  uint64_t getRN(uint64_t pn) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling getRN() multiple times. */
  void getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const;

  /** Select rows by routing number prefix.
    * Use cursor methods to retrieve relevent rows. */
  PhoneMapping& inverseRNs(uint64_t fromRN, uint64_t toRN) &;
  PhoneMapping&& inverseRNs(uint64_t fromRN, uint64_t toRN) &&;

  /** Select all rows. Use cursor methods to retrieve relevent rows.*/
  PhoneMapping& visitRows() &;
  PhoneMapping&& visitRows() &&;

  /** Has cursor point to some row? */
  bool hasRow() const noexcept;

  /** Retrieve portability number from the current row. */
  uint64_t currentPN() const noexcept;

  /** Retrieve routing number from the current row. */
  uint64_t currentRN() const noexcept;

  /** Move to next row. */
  PhoneMapping& advance() noexcept;

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
  std::unique_ptr<Cursor> cursor_;
};

#endif // CALLFWD_PHONEMAPPING_H
