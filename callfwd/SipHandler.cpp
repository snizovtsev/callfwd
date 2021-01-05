//#include "SipHandler.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/AsyncUDPServerSocket.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "PhoneMapping.h"

extern "C" {
#include <lib/osips_parser/msg_parser.h>
#include <lib/osips_parser/parse_uri.h>
#include <lib/osips_parser/contact/parse_contact.h>
}

using proxygen::RequestHandlerFactory;
using proxygen::RequestHandler;
using proxygen::HTTPMessage;
using folly::StringPiece;

DEFINE_uint32(sip_port, 5061, "Port to listen on with SIP protocol");
DEFINE_string(sip_if1, "0.0.0.0", "IP/Hostname to bind SIP to");

static StringPiece ToStringPiece(str s) {
  return StringPiece(s.s, s.len);
}

class UDPAcceptor : public folly::AsyncUDPServerSocket::Callback {
 public:
  UDPAcceptor(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
  {
  }

  void onListenStarted() noexcept override {}

  void onListenStopped() noexcept override {}

  void onDataAvailable(
      std::shared_ptr<folly::AsyncUDPSocket> socket,
      const folly::SocketAddress& client,
      std::unique_ptr<folly::IOBuf> data,
      bool /*unused*/,
      OnDataAvailableParams /*unused*/) noexcept override
  {
    if (data->length() == 0)
      return;

    memset(&msg_, 0, sizeof(msg_));
    msg_.buf = (char*) data->data();
    msg_.len = data->length();

    if (parse_msg(msg_.buf, msg_.len, &msg_) != 0) {
      LOG(INFO) << "Failed to parse SIP message";
      return;
    }

    if (msg_.first_line.type != SIP_REQUEST)
      return;

    std::string reply;
    auto copyHeader = [&reply](struct hdr_field* hdr) {
      if (hdr) {
        reply += ToStringPiece(hdr->name);
        reply += ": ";
        reply += ToStringPiece(hdr->body);
        reply += "\r\n";
      }
    };

    if (msg_.first_line.u.request.method_value == METHOD_OPTIONS) {
      reply += "SIP/2.0 200 OK\r\n";
      copyHeader(msg_.h_via1);
      copyHeader(msg_.from);
      copyHeader(msg_.to);
      copyHeader(msg_.callid);
      copyHeader(msg_.cseq);
      reply += "Content-Length: 0\r\n\r\n";
      socket->write(client, folly::IOBuf::copyBuffer(std::move(reply)));
      return;
    }

    if (msg_.first_line.u.request.method_value != METHOD_INVITE)
      return;

    if (parse_sip_msg_uri(&msg_) != 0) {
      LOG(INFO) << "Failed to parse SIP URI";
      return;
    }

    StringPiece user = ToStringPiece(msg_.parsed_uri.user);
    if (!user.removePrefix("+1"))
      return;
    uint64_t userPhone;
    if (auto result = folly::tryTo<uint64_t>(user)) {
      userPhone = result.value();
    } else {
      return;
    }

    reply += "SIP/2.0 302 Moved Temporarily\r\n";
    copyHeader(msg_.h_via1);
    copyHeader(msg_.from);
    copyHeader(msg_.to);
    copyHeader(msg_.callid);
    copyHeader(msg_.cseq);

    uint64_t targetPhone = db_->findTarget(userPhone);
    if (targetPhone == PhoneMapping::NONE)
      targetPhone = userPhone;
    std::string target = "+1";
    target += folly::to<std::string>(targetPhone);

    reply += "Contact: <";
    reply += "sip:+1";
    reply += user;
    reply += ";rn=";
    reply += target;
    reply += ";ndpi@";
    reply += ToStringPiece(msg_.parsed_uri.host);
    reply += ':';
    reply += ToStringPiece(msg_.parsed_uri.port);
    reply += ">\r\n";

    reply += "Location-Info: N\r\n"
             "Content-Length: 0\r\n"
             "\r\n";
    socket->write(client, folly::IOBuf::copyBuffer(std::move(reply)));
  }

 private:
  std::shared_ptr<PhoneMapping> db_;
  struct sip_msg msg_;
};

class SipHandlerFactory : public proxygen::RequestHandlerFactory {
 public:
  explicit SipHandlerFactory(std::shared_ptr<PhoneMapping> db)
    : db_(std::move(db))
  {
    folly::EventBase* evb = folly::EventBaseManager::get()->getEventBase();
    socket_ = std::make_unique<folly::AsyncUDPServerSocket>(evb);
    try {
      socket_->bind(folly::SocketAddress(FLAGS_sip_if1, FLAGS_sip_port));
      LOG(INFO) << "Server listening on " << socket_->address().describe();
    } catch (const std::exception& ex) {
      LOG(FATAL) << ex.what();
    }
    socket_->listen();
  }

  void onServerStart(folly::EventBase* evb) noexcept override {
    // Add UDP handler here
    socket_->addListener(evb, new UDPAcceptor(db_));
  }

  void onServerStop() noexcept override {
    // Stop UDP handler
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    return upstream;
  }

 private:
  std::shared_ptr<PhoneMapping> db_;
  std::unique_ptr<folly::AsyncUDPServerSocket> socket_;
};


std::unique_ptr<RequestHandlerFactory>
makeSipHandlerFactory(std::shared_ptr<PhoneMapping> db)
{
  return std::make_unique<SipHandlerFactory>(std::move(db));
}
