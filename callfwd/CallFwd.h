#ifndef CALLFWD_H
#define CALLFWD_H

#include <sstream>
#include <ctime>
#include <memory>
#include <folly/Range.h>

class PhoneMapping;
namespace folly {
  class AsyncUDPSocket;
  class AsyncLogWriter;
  class SocketAddress;
  class IPAddress;
}
namespace proxygen {
  class RequestHandlerFactory;
}

class AccessLogFormatter {
 public:
  explicit AccessLogFormatter(std::shared_ptr<folly::AsyncLogWriter> log);

  void onRequest(const folly::SocketAddress &peer,
                 folly::StringPiece method, folly::StringPiece uri,
                 time_t startTime);

  void onResponse(size_t status, size_t bytes);

 private:
  std::shared_ptr<folly::AsyncLogWriter> log_;
  std::ostringstream message_;
};

void startControlSocket();
bool loadMappingFile(const char* fname);
std::shared_ptr<PhoneMapping> getPhoneMapping();
int checkACL(const folly::IPAddress &peer);

std::unique_ptr<proxygen::RequestHandlerFactory>
makeApiHandlerFactory();

std::unique_ptr<proxygen::RequestHandlerFactory>
makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPSocket>> socket,
                      std::shared_ptr<folly::AsyncLogWriter> log);

std::unique_ptr<proxygen::RequestHandlerFactory>
makeAccessLogHandlerFactory(std::shared_ptr<folly::AsyncLogWriter> log);

#endif // CALLFWD_H
