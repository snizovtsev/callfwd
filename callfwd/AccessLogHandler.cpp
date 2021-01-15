#include <sstream>
#include <iomanip>
#include <folly/portability/GFlags.h>
#include <folly/Format.h>
#include <folly/logging/LogWriter.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "CallFwd.h"

using proxygen::RequestHandler;
using proxygen::RequestHandlerFactory;
using proxygen::HTTPMessage;
using folly::StringPiece;

void AccessLogFormatter::onRequest(const folly::SocketAddress &peer,
                                   StringPiece method, StringPiece uri,
                                   time_t startTime)
{
  const char* datefmt = "%d/%b/%Y:%H:%M:%S %z";
  struct tm date;

  message_.clear();
  gmtime_r(&startTime, &date);
  message_ << peer << " - - " << "[" << std::put_time(&date, datefmt) << "] \""
            << method << " " << uri << "\" ";
}

void AccessLogFormatter::onResponse(size_t status, size_t bytes)
{
  message_ << status << " " << bytes << "\n";
  if (auto log = getAccessLogWriter())
    log->writeMessage(std::move(message_).str());
}

class AccessLogHandler final : public proxygen::Filter {
 public:
  explicit AccessLogHandler(RequestHandler* upstream)
    : Filter(upstream)
  {}

  void onRequest(std::unique_ptr<HTTPMessage> msg) noexcept override {
    log_.onRequest(msg->getClientAddress(), msg->getMethodString(), msg->getURL(),
                   proxygen::toTimeT(msg->getStartTime()));
    Filter::onRequest(std::move(msg));
  }

  void sendHeaders(HTTPMessage& msg) noexcept override {
    status_ = msg.getStatusCode();
    Filter::sendHeaders(msg);
  }

  void sendBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    bytes_ += body->computeChainDataLength();
    Filter::sendBody(std::move(body));
  }

  void requestComplete() noexcept override {
    log_.onResponse(status_, bytes_);
    delete this;
  }

 private:
  AccessLogFormatter log_;
  uint32_t status_ = 0;
  size_t bytes_ = 0;
};

class AccessLogHandlerFactory : public RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    if (!getAccessLogWriter())
      return upstream;
    return new AccessLogHandler(upstream);
  }
};

std::unique_ptr<RequestHandlerFactory> makeAccessLogHandlerFactory()
{
  return std::make_unique<AccessLogHandlerFactory>();
}
