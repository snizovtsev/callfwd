#include <gflags/gflags.h>
#include <glog/logging.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/AsyncUDPSocket.h>
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
using folly::StringPiece;
using folly::NetworkSocket;
using folly::AsyncUDPSocket;
using folly::EventBase;

static StringPiece ToStringPiece(str s) {
  return StringPiece(s.s, s.len);
}

std::string formatTime(proxygen::TimePoint tp);

class SIPHandler : public AsyncUDPSocket::ReadCallback {
 public:
  explicit SIPHandler(EventBase *evb, NetworkSocket sockfd)
    : socket_(evb)
    , recvbuf_(1500)
  {
    socket_.setFD(sockfd, AsyncUDPSocket::FDOwnership::SHARED);
    socket_.resumeRead(this);
  }

  void copyHeader(const struct hdr_field* hdr) {
    if (hdr) {
      reply_.append(ToStringPiece(hdr->name));
      reply_.append(": ");
      reply_.append(rtrimWhitespace(ToStringPiece(hdr->body)));
      reply_.append("\r\n");
    }
  }

  void getReadBuffer(void** buf, size_t* len) noexcept override {
    *buf = recvbuf_.data();
    *len = recvbuf_.size();
  }

  void onReadError(const folly::AsyncSocketException& ex) noexcept override {
    LOG(ERROR) << ex.what();
    socket_.resumeRead(this);
  }

  void onReadClosed() noexcept override {
    delete this;
  }

  void onDataAvailable(
      const folly::SocketAddress& client,
      size_t len, bool /*truncated*/,
      OnDataAvailableParams /*params*/) noexcept override
  {
    if (len == 0)
      return;

    status_ = 0;
    memset(&msg_, 0, sizeof(msg_));
    msg_.buf = recvbuf_.data();
    msg_.len = len;
    reply_.clear();

    auto startTime = proxygen::TimePoint::clock::now();
    if (parse_msg(msg_.buf, msg_.len, &msg_) != 0) {
      LOG(INFO) << "Failed to parse SIP message";
      return;
    }

    if (msg_.first_line.type != SIP_REQUEST)
      return;
    auto &req = msg_.first_line.u.request;

    switch (req.method_value) {
    case METHOD_OPTIONS:
      replyWithStatus(200, "OK", client);
      break;
    case METHOD_INVITE:
      handleInvite(client);
      break;
    default:
      break;
    }

    if (VLOG_IS_ON(1)) {
      google::LogMessage("", 0).stream()
        << client << " - - "
        << "[" << formatTime(startTime) << "] \""
        << ToStringPiece(req.method) << " "
        << ToStringPiece(req.uri) << "\" "
        << status_ << " 0";
    }
  }

  void replyWithStatus(uint64_t status, StringPiece message,
                       const folly::SocketAddress& client)
  {
      status_ = status;
      reply_.append("SIP/2.0 ");
      reply_.append(folly::to<std::string>(status));
      reply_.append(" ");
      reply_.append(message);
      reply_.append("\r\n");
      copyHeader(msg_.h_via1);
      copyHeader(msg_.from);
      copyHeader(msg_.to);
      copyHeader(msg_.callid);
      copyHeader(msg_.cseq);
      reply_.append("Content-Length: 0\r\n\r\n");
      socket_.writeChain(client, reply_.move(), {});
  }

  void handleInvite(const folly::SocketAddress& client)
  {
    switch (checkACL(client.getIPAddress())) {
    case 429: return replyWithStatus(429, "Too Many Requests", client);
    case 401: return replyWithStatus(401, "Unauthorized", client);
    default: break;
    }

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
    socket_.writeChain(client, reply_.move(), {});
  }

 private:
  folly::AsyncUDPSocket socket_;
  std::vector<char> recvbuf_;
  folly::IOBufQueue reply_;

  uint64_t status_;
  struct sip_msg msg_;
};

class SipHandlerFactory : public RequestHandlerFactory {
 public:
  explicit SipHandlerFactory(std::vector<std::shared_ptr<AsyncUDPSocket>> udpServer)
    : server_(std::move(udpServer))
  {
  }

  void onServerStart(folly::EventBase* evb) noexcept override {
    // Add UDP handler here
    for (auto socket : server_) {
      new SIPHandler(evb, socket->getNetworkSocket());
      //socket->addListener(evb, new SIPHandler);
    }
  }

  void onServerStop() noexcept override {
    // Stop UDP handler
  }

  RequestHandler* onRequest(RequestHandler *upstream, proxygen::HTTPMessage*) noexcept override {
    return upstream;
  }

 private:
  std::vector<std::shared_ptr<AsyncUDPSocket>> server_;
};


std::unique_ptr<RequestHandlerFactory> makeSipHandlerFactory(std::vector<std::shared_ptr<AsyncUDPSocket>> udpServer)
{
  return std::make_unique<SipHandlerFactory>(std::move(udpServer));
}
