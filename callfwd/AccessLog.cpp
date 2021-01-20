#include "AccessLog.h"

#include <sstream>
#include <iomanip> // put_time
#include <memory>
#include <atomic>
#include <system_error>
#include <sys/signal.h>

#include <glog/logging.h>
#include <folly/Format.h>
#include <folly/portability/GFlags.h>
#include <folly/logging/LogWriter.h>
#include <folly/logging/AsyncFileWriter.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/synchronization/HazptrHolder.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

using folly::StringPiece;
using proxygen::RequestHandler;
using proxygen::RequestHandlerFactory;
using proxygen::HTTPMessage;

DEFINE_string(access_log, "/tmp/callfwd.log", "An access log file");

class AccessLogWriter : public folly::AsyncFileWriter
                      , public folly::hazptr_obj_base<AccessLogWriter>
{
 public:
  using folly::AsyncFileWriter::AsyncFileWriter;
};
static std::atomic<AccessLogWriter*> accessLog;

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
  folly::hazptr_holder h;
  if (auto log = h.get_protected(accessLog))
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
    if (!accessLog.load())
      return upstream;
    return new AccessLogHandler(upstream);
  }
};

class AccessLogRotator : public folly::AsyncSignalHandler {
 public:
  explicit AccessLogRotator(folly::EventBase *evb)
    : folly::AsyncSignalHandler(evb)
  {
    signalReceived(0);
    registerSignalHandler(SIGHUP);
  }

  void signalReceived(int /*signum*/) noexcept override
  try {
    if (FLAGS_access_log.empty())
      return;

    LOG(INFO) << "SIGHUP received: rotating " << FLAGS_access_log;
    auto recruit = std::make_unique<AccessLogWriter>(FLAGS_access_log);
    auto veteran = accessLog.exchange(recruit.release());
    if (veteran) {
      veteran->flush();
      veteran->retire();
    }
  } catch (std::system_error& e) {
    LOG(ERROR) << "Could not open log file: " << e.what();
  }

  ~AccessLogRotator() {
    if (auto veteran = accessLog.exchange(nullptr)) {
      LOG(INFO) << "Flushing access log";
      veteran->flush();
      veteran->retire();
    }
  }
};

std::shared_ptr<AccessLogRotator> makeAccessLogRotator(folly::EventBase *evb)
{
  return std::make_shared<AccessLogRotator>(evb);
}

std::unique_ptr<RequestHandlerFactory> makeAccessLogHandlerFactory()
{
  return std::make_unique<AccessLogHandlerFactory>();
}
