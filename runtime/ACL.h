#ifndef CALLFWD_ACL_H
#define CALLFWD_ACL_H

#include <istream>
#include <atomic>
#include <memory>
#include <folly/synchronization/HazptrHolder.h>

#include "CallFwd.h"

class ACL {
 public:
  class Rule;
  class Data;

  /** Construct taking ownership of Data. */
  ACL(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  ACL(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  ACL(ACL&& rhs) noexcept;
  /** Get default instance from global variable. */
  static ACL get() noexcept;
  ~ACL() noexcept;

  /* Check if peer is allowed at the moment */
  int isCallAllowed(const folly::IPAddress &peer) const;

  /* Construct Data from CSV stream */
  static std::unique_ptr<ACL::Data> fromCSV(std::istream &in, size_t &line);

  /* Exchange current ACL with global */
  static void commit(std::unique_ptr<ACL::Data> recruit,
                     std::atomic<Data*> &global);

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
};

namespace std {
  template<>
  struct default_delete<ACL::Data> {
    void operator()(ACL::Data *ptr) const;
  };
}

#endif // CALLFWD_ACL_H
