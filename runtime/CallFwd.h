#ifndef CALLFWD_CALLFWD_H
#define CALLFWD_CALLFWD_H

#include <sstream>
#include <ctime>
#include <memory>
#include <vector>

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

void startControlSocket();

std::unique_ptr<proxygen::RequestHandlerFactory>
makeApiHandlerFactory();

std::unique_ptr<proxygen::RequestHandlerFactory>
makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPSocket>> socket);

std::unique_ptr<proxygen::RequestHandlerFactory>
makeAccessLogHandlerFactory();

#endif // CALLFWD_CALLFWD_H
