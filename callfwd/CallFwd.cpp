#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/portability/GFlags.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "Control.h"


using proxygen::HTTPServer;
using proxygen::HTTPServerOptions;
using proxygen::RequestHandlerChain;
using proxygen::RequestHandlerFactory;

DEFINE_uint32(http_port, 11000, "Port to listen on with HTTP protocol");
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

std::unique_ptr<RequestHandlerFactory> makeApiHandlerFactory();
std::unique_ptr<RequestHandlerFactory> makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPServerSocket>> udpServer);
std::unique_ptr<RequestHandlerFactory> makeAccessLogHandlerFactory();

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
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
      IPs.emplace_back(folly::SocketAddress{intf, FLAGS_http_port, true},
                       HTTPServer::Protocol::HTTP);
    }
  }

  std::vector<std::shared_ptr<folly::AsyncUDPServerSocket>> udpServer;
  folly::EventBase* evb = folly::EventBaseManager::get()->getEventBase();
  for (std::string intf : {FLAGS_sip_if1, FLAGS_sip_if2, FLAGS_sip_if3, FLAGS_sip_if4}) {
    if (!intf.empty()) {
      auto socket = std::make_shared<folly::AsyncUDPServerSocket>(evb);
      socket->bind(folly::SocketAddress(intf, FLAGS_sip_port));
      LOG(INFO) << "SIP listening on " << socket->address().describe();
      udpServer.push_back(std::move(socket));
    }
  }

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  options.idleTimeout = std::chrono::milliseconds(60000);
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

  loadMappingFile(argv[1]);

  LOG(INFO) << "Serving requests";
  for (auto socket : udpServer)
    socket->listen();
  server.start();
  return 0;
}
