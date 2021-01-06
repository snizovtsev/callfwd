#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>
#include <memory>
#include <limits>

struct PhoneList {
  uint64_t phone;
  PhoneList *next;
};

class PhoneMapping : public std::enable_shared_from_this<PhoneMapping> {
 public:
  enum {
    NONE = std::numeric_limits<uint64_t>::max(),
  };

  size_t size() const { return sourceNumbers_.size(); }
  uint64_t findTarget(uint64_t source) const;
  std::vector<uint64_t> reverseTarget(uint64_t targetMin, uint64_t targetMax) const;

 protected:
  friend class PhoneMappingDumper;
  static std::shared_ptr<PhoneMapping> detach(PhoneMapping &b);
  std::unordered_map<uint64_t, uint64_t> targetMapping_;
  std::vector<PhoneList> sourceNumbers_;
  std::vector<PhoneList> sortedTargets_;
};

class PhoneMappingBuilder : protected PhoneMapping {
 public:
  void SizeHint(uint64_t numRecords);
  PhoneMappingBuilder& addMapping(uint64_t source, uint64_t target);
  std::shared_ptr<PhoneMapping> build();
};

class PhoneMappingDumper {
 public:
  PhoneMappingDumper(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
    , rowIter_(db_->sourceNumbers_.cbegin())
  {
  }

  uint64_t currentSource() const {
    return rowIter_->phone;
  }

  uint64_t currentTarget() const {
    return db_->findTarget(currentSource());
  }

  bool hasNext() const { return rowIter_ != db_->sourceNumbers_.cend(); }
  void moveNext() { ++rowIter_; }

 private:
  std::shared_ptr<PhoneMapping> db_;
  std::vector<PhoneList>::const_iterator rowIter_;
};
