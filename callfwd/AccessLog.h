#ifndef CALLFWD_ACCESS_LOG_H
#define CALLFWD_ACCESS_LOG_H

#include "CallFwd.h"
#include <folly/Range.h>

class AccessLogRotator;

class AccessLogFormatter {
 public:
  void onRequest(const folly::SocketAddress &peer,
                 folly::StringPiece method, folly::StringPiece uri,
                 time_t startTime);

  void onResponse(size_t status, size_t bytes);

 private:
  std::ostringstream message_;
};

std::shared_ptr<AccessLogRotator>
makeAccessLogRotator(folly::EventBase *evb);

#endif // CALLFWD_ACCESS_LOG_H
