#include <gflags/gflags.h>
#include <glog/logging.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/AsyncUDPServerSocket.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "PhoneMapping.h"
#include "Control.h"

extern "C" {
#include <lib/osips_parser/msg_parser.h>
#include <lib/osips_parser/parse_uri.h>
#include <lib/osips_parser/contact/parse_contact.h>
}

using proxygen::RequestHandlerFactory;
using proxygen::RequestHandler;
using proxygen::HTTPMessage;
using folly::StringPiece;

static StringPiece ToStringPiece(str s) {
  return StringPiece(s.s, s.len);
}

std::string formatTime(proxygen::TimePoint tp);

class UDPAcceptor : public folly::AsyncUDPServerSocket::Callback {
 public:
  void onListenStarted() noexcept override {}

  void onListenStopped() noexcept override {}

  void copyHeader(const struct hdr_field* hdr) {
    if (hdr) {
      reply_.append(ToStringPiece(hdr->name));
      reply_.append(": ");
      reply_.append(ToStringPiece(hdr->body));
      reply_.append("\r\n");
    }
  }

  void onDataAvailable(
      std::shared_ptr<folly::AsyncUDPSocket> socket,
      const folly::SocketAddress& client,
      std::unique_ptr<folly::IOBuf> data,
      bool /*unused*/,
      OnDataAvailableParams /*unused*/) noexcept override
  {
    if (data->length() == 0)
      return;

    auto startTime = proxygen::TimePoint::clock::now();
    memset(&msg_, 0, sizeof(msg_));
    msg_.buf = (char*) data->writableBuffer();
    msg_.len = data->length();
    reply_.clear();

    if (parse_msg(msg_.buf, msg_.len, &msg_) != 0) {
      LOG(INFO) << "Failed to parse SIP message";
      return;
    }

    if (msg_.first_line.type != SIP_REQUEST)
      return;
    auto &req = msg_.first_line.u.request;

    switch (req.method_value) {
    case METHOD_OPTIONS:
      handleOptions(std::move(socket), client);
      break;
    case METHOD_INVITE:
      handleInvite(std::move(socket), client);
      break;
    default:
      break;
    }

    google::LogMessage("", 0).stream()
      << client << " - - "
      << "[" << formatTime(startTime) << "] \""
      << ToStringPiece(req.method) << " " << ToStringPiece(req.uri) << "\" "
      << status_ << " 0";
  }

  void handleOptions(std::shared_ptr<folly::AsyncUDPSocket> socket,
                     const folly::SocketAddress& client)
  {
      status_ = 200;
      reply_.append("SIP/2.0 200 OK\r\n");
      copyHeader(msg_.h_via1);
      copyHeader(msg_.from);
      copyHeader(msg_.to);
      copyHeader(msg_.callid);
      copyHeader(msg_.cseq);
      reply_.append("Content-Length: 0\r\n\r\n");
      socket->write(client, reply_.move());
  }

  void handleInvite(std::shared_ptr<folly::AsyncUDPSocket> socket,
                    const folly::SocketAddress& client)
  {
    if (parse_sip_msg_uri(&msg_) != 0) {
      LOG(INFO) << "Failed to parse SIP URI";
      return;
    }

    StringPiece user = ToStringPiece(msg_.parsed_uri.user);
    if (!(user.removePrefix("+1") || user.removePrefix("1")))
      return;
    uint64_t userPhone;
    if (auto result = folly::tryTo<uint64_t>(user)) {
      userPhone = result.value();
    } else {
      return;
    }

    status_ = 302;
    reply_.append("SIP/2.0 302 Moved Temporarily\r\n");
    copyHeader(msg_.h_via1);
    copyHeader(msg_.from);
    copyHeader(msg_.to);
    copyHeader(msg_.callid);
    copyHeader(msg_.cseq);

    auto db = getPhoneMapping();
    uint64_t targetPhone = db->findTarget(userPhone);
    if (targetPhone == PhoneMapping::NONE)
      targetPhone = userPhone;
    std::string target = "+1";
    target += folly::to<std::string>(targetPhone);

    reply_.append("Contact: <sip:+1");
    reply_.append(user);
    reply_.append(";rn=");
    reply_.append(target);
    reply_.append(";ndpi@");
    reply_.append(ToStringPiece(msg_.parsed_uri.host));
    reply_.append(":");
    reply_.append(ToStringPiece(msg_.parsed_uri.port));
    reply_.append(">\r\n"
                  "Location-Info: N\r\n"
                  "Content-Length: 0\r\n\r\n");
    socket->write(client, reply_.move());
  }

 private:
  folly::IOBufQueue reply_;
  uint64_t status_;
  struct sip_msg msg_;
};

class SipHandlerFactory : public proxygen::RequestHandlerFactory {
 public:
  explicit SipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPServerSocket>> udpServer)
    : server_(std::move(udpServer))
  {
  }

  void onServerStart(folly::EventBase* evb) noexcept override {
    // Add UDP handler here
    for (auto socket : server_)
      socket->addListener(evb, new UDPAcceptor);
  }

  void onServerStop() noexcept override {
    // Stop UDP handler
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    return upstream;
  }

 private:
  std::vector<std::shared_ptr<folly::AsyncUDPServerSocket>> server_;
};


std::unique_ptr<RequestHandlerFactory> makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPServerSocket>> udpServer)
{
  return std::make_unique<SipHandlerFactory>(std::move(udpServer));
}
