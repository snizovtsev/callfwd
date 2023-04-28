#include <folly/init/Init.h>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/portability/GFlags.h>
#include <folly/system/HardwareConcurrency.h>
#include <folly/logging/AsyncFileWriter.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "CallFwd.h"
#include "AccessLog.h"

using proxygen::HTTPServer;
using proxygen::HTTPServerOptions;
using proxygen::RequestHandlerChain;
using proxygen::RequestHandlerFactory;

DEFINE_uint32(http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_uint32(http_idle_timeout, 60, "A timeout to close inactive sessions");
DEFINE_string(http_if1, "localhost", "IP/Hostname to bind HTTP to");
DEFINE_string(http_if2, "", "Additional address to bind HTTP to");
DEFINE_string(http_if3, "", "Additional address to bind HTTP to");
DEFINE_uint32(sip_port, 5061, "Port to listen on with SIP protocol");
DEFINE_string(sip_if1, "127.0.0.1", "IP/Hostname to bind SIP to");
DEFINE_string(sip_if2, "::1", "IP/Hostname to bind SIP to");
DEFINE_string(sip_if3, "", "IP/Hostname to bind SIP to");
DEFINE_string(sip_if4, "", "IP/Hostname to bind SIP to");
DEFINE_int32(threads, 0,
             "Number of threads to listen on. Numbers <= 0 "
             "will use the number of cores on this machine.");

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);
  google::InstallFailureSignalHandler();
  setlocale(LC_ALL, "C");
  startControlSocket();

  CHECK(FLAGS_http_port < 65536);
  if (FLAGS_threads <= 0) {
    FLAGS_threads = folly::hardware_concurrency();
    CHECK(FLAGS_threads > 0);
  }

  std::vector<HTTPServer::IPConfig> IPs;
  for (std::string intf : {FLAGS_http_if1, FLAGS_http_if2, FLAGS_http_if3}) {
    if (!intf.empty()) {
      uint16_t port = FLAGS_http_port;
      IPs.emplace_back(folly::SocketAddress{intf, port, true},
                       HTTPServer::Protocol::HTTP);
    }
  }

  std::vector<std::shared_ptr<folly::AsyncUDPSocket>> udpServer;
  folly::EventBase* evb = folly::EventBaseManager::get()->getEventBase();
  for (std::string intf : {FLAGS_sip_if1, FLAGS_sip_if2, FLAGS_sip_if3, FLAGS_sip_if4}) {
    if (!intf.empty()) {
      auto socket = std::make_shared<folly::AsyncUDPSocket>(evb);
      socket->bind(folly::SocketAddress(intf, FLAGS_sip_port));
      LOG(INFO) << "SIP listening on " << socket->address().describe();
      udpServer.push_back(std::move(socket));
    }
  }

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  options.idleTimeout = std::chrono::seconds(FLAGS_http_idle_timeout);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.enableContentCompression = false;
  options.h2cEnabled = false;
  // Increase the default flow control to 1MB/10MB
  options.initialReceiveWindow = uint32_t(1 << 20);
  options.receiveStreamWindowSize = uint32_t(1 << 20);
  options.receiveSessionWindowSize = 10 * (1 << 20);
  options.handlerFactories = RequestHandlerChain()
    .addThen(makeAccessLogHandlerFactory())
    .addThen(makeApiHandlerFactory())
    .addThen(makeSipHandlerFactory(udpServer))
    .build();

  HTTPServer server(std::move(options));
  LOG(INFO) << "Starting HTTP server on port " << FLAGS_http_port;
  server.bind(IPs);

  LOG(INFO) << "Serving requests";
  auto logRotator = makeAccessLogRotator(evb);
  server.start();
  for (auto& sock : udpServer) {
    sock->close();
  }
  return 0;
}
