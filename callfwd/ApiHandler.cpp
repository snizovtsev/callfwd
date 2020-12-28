#include "ApiHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/filters/DirectResponseHandler.h>

#include "PhoneMapping.h"


std::shared_ptr<PhoneMapping> db_ = PhoneMappingBuilder().build();


using namespace proxygen;
using folly::StringPiece;

class TargetHandler final : public RequestHandler {
 public:
  explicit TargetHandler(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
  {
  }

  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    HTTPMessage::splitNameValuePieces(req->getQueryStringAsStringPiece(), '&', '=',
                                      std::bind(&TargetHandler::onQueryParam, this,
                                                std::placeholders::_1,
                                                std::placeholders::_2));

    for (uint64_t phone : query_) {
      uint64_t target = db_->findTarget(phone);
      if (target != PhoneMapping::NONE)
        resp_.push_back(target);
      else
        resp_.push_back(phone);
    }

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "text/plain")
      .body(folly::join(",", resp_))
      .send();
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "phone%5B%5D") {
      auto intValue = folly::tryTo<uint64_t>(value);
      if (intValue.hasValue())
        query_.push_back(intValue.value());
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
  }

  void onEOM() noexcept override {
    ResponseBuilder(downstream_).sendWithEOM();
  }

  void onUpgrade(UpgradeProtocol proto) noexcept override {
    // handler doesn't support upgrades
  }

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(ProxygenError err) noexcept override {
    delete this;
  }

 private:
  std::vector<uint64_t> query_;
  std::vector<uint64_t> resp_;
  std::unique_ptr<folly::IOBuf> body_;
  std::shared_ptr<PhoneMapping> db_;
};

class ReverseHandler final : public RequestHandler {
 public:
  explicit ReverseHandler(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
  {
  }

  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    HTTPMessage::splitNameValuePieces(req->getQueryStringAsStringPiece(), '&', '=',
                                      std::bind(&ReverseHandler::onQueryParam, this,
                                                std::placeholders::_1,
                                                std::placeholders::_2));

    for (std::pair<uint64_t, uint64_t> range : query_) {
      auto phones = db_->reverseTarget(range.first, range.second);
      std::copy(phones.begin(), phones.end(), std::back_inserter(resp_));
    }

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "text/plain")
      .body(folly::join(",", resp_))
      .send();
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "prefix%5B%5D" && value.size() <= 10) {
      uint64_t from, to;
      auto intValue = folly::tryTo<uint64_t>(value);
      if (!intValue.hasValue())
        return;

      from = intValue.value();
      to = from + 1;

      for (size_t i = 0; i < 10 - value.size(); ++i) {
        from *= 10;
        to *= 10;
      }
      query_.emplace_back(from, to);
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
  }

  void onEOM() noexcept override {
    ResponseBuilder(downstream_).sendWithEOM();
  }

  void onUpgrade(UpgradeProtocol proto) noexcept override {
    // handler doesn't support upgrades
  }

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(ProxygenError err) noexcept override {
    delete this;
  }

 private:
  std::shared_ptr<PhoneMapping> db_;
  std::vector<std::pair<uint64_t, uint64_t>> query_;
  std::vector<uint64_t> resp_;
};

class ApiHandlerFactory : public RequestHandlerFactory {
 public:
  explicit ApiHandlerFactory(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
  {
  }

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    bool isGET = msg->getMethod() == HTTPMethod::GET;
    folly::StringPiece path = msg->getPathAsStringPiece();

    if (isGET && path == "/target") {
      return new TargetHandler(db_);
    } else if (isGET && path == "/reverse") {
      return new ReverseHandler(db_);
    }

    return new DirectResponseHandler(404, "Not found", "");
  }

 private:
  std::shared_ptr<PhoneMapping> db_;
};

std::unique_ptr<RequestHandlerFactory>
makeApiHandlerFactory(std::shared_ptr<PhoneMapping> db)
{
  return std::make_unique<ApiHandlerFactory>(std::move(db));
}
