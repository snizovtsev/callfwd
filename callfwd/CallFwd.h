#ifndef CALLFWD_H
#define CALLFWD_H

#include <sstream>
#include <ctime>
#include <memory>
#include <folly/Range.h>

class AccessLogRotator;
namespace folly {
  class AsyncUDPSocket;
  class SocketAddress;
  class IPAddress;
  class LogWriter;
  class EventBase;
}
namespace proxygen {
  class RequestHandlerFactory;
}

class AccessLogFormatter {
 public:
  void onRequest(const folly::SocketAddress &peer,
                 folly::StringPiece method, folly::StringPiece uri,
                 time_t startTime);

  void onResponse(size_t status, size_t bytes);

 private:
  std::ostringstream message_;
};

void startControlSocket();
bool loadMappingFile(const char* fname);
std::shared_ptr<folly::LogWriter> getAccessLogWriter();
int checkACL(const folly::IPAddress &peer);

std::unique_ptr<proxygen::RequestHandlerFactory>
makeApiHandlerFactory();

std::unique_ptr<proxygen::RequestHandlerFactory>
makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPSocket>> socket);

std::unique_ptr<proxygen::RequestHandlerFactory>
makeAccessLogHandlerFactory();

std::shared_ptr<AccessLogRotator>
makeAccessLogRotator(folly::EventBase *evb);

#endif // CALLFWD_H
