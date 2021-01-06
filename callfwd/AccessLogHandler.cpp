#include <gflags/gflags.h>
#include <glog/logging.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

using namespace proxygen;
using folly::StringPiece;

std::string formatTime(TimePoint tp) {
  time_t t = toTimeT(tp);
  struct tm final_tm;
  gmtime_r(&t, &final_tm);
  char buf[256];
  if (strftime(buf, sizeof(buf), "%d/%b/%Y:%H:%M:%S %z", &final_tm) > 0) {
    return std::string(buf);
  }
  return "";
}

class AccessLogHandler final : public Filter {
 public:
  explicit AccessLogHandler(RequestHandler* upstream)
    : Filter(upstream)
    , log_("", 0)
  {}

  void onRequest(std::unique_ptr<HTTPMessage> msg) noexcept override {
    const folly::SocketAddress &peer = msg->getClientAddress();
    const std::string &url = msg->getURL();
    const std::string &method = msg->getMethodString();
    std::string startTime = formatTime(msg->getStartTime());

    log_.stream()
      << peer << " - - " << "[" << startTime << "] \""
      << method << " " << url << "\" ";

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
    log_.stream() << status_ << " " << bytes_;
    delete this;
  }

 private:
  google::LogMessage log_;
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
    if (!VLOG_IS_ON(1))
      return upstream;
    return new AccessLogHandler(upstream);
  }
};

std::unique_ptr<RequestHandlerFactory> makeAccessLogHandlerFactory()
{
  return std::make_unique<AccessLogHandlerFactory>();
}
