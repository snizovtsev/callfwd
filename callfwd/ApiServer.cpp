#include <fstream>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/portability/GFlags.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "PhoneMapping.h"


using proxygen::HTTPServer;
using proxygen::HTTPServerOptions;
using proxygen::RequestHandlerChain;
using proxygen::RequestHandlerFactory;

DEFINE_uint32(http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_string(http_if1, "localhost", "IP/Hostname to bind HTTP to");
DEFINE_string(http_if2, "", "Additional address to bind HTTP to");
DEFINE_string(http_if3, "", "Additional address to bind HTTP to");
DEFINE_int32(threads,
             0,
             "Number of threads to listen on. Numbers <= 0 "
             "will use the number of cores on this machine.");

std::unique_ptr<RequestHandlerFactory>
makeApiHandlerFactory(std::shared_ptr<PhoneMapping> db);

std::unique_ptr<RequestHandlerFactory>
makeSipHandlerFactory(std::shared_ptr<PhoneMapping> db);

std::shared_ptr<PhoneMapping> loadMappingFile(const char* fname)
{
  PhoneMappingBuilder builder;
  std::string line;
  std::vector<uint64_t> row;
  size_t nrows = 0;

  struct stat fstat;
  std::vector<char> rbuf(1ull << 19);
  std::ifstream in(fname);

  if (in.fail()) {
    LOG(FATAL) << "Failed to open " << fname;
  }

  memset(&fstat, 0, sizeof(fstat));
  if (lstat(fname, &fstat) == 0) {
    if (fstat.st_size > 1000) {
      size_t lineEstimate = fstat.st_size / 23;
      builder.SizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }


  LOG(INFO) << "Reading database from " << fname << " ...";
  in.rdbuf()->pubsetbuf(rbuf.data(), rbuf.size());
  while (getline(in, line)) {
    folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
    builder.addMapping(row[0], row[1]);
    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    LOG(FATAL) << "Read failed on line " << nrows;
  }
  in.close();

  LOG(INFO) << "Building index (" << nrows << " rows)...";
  return builder.build();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  setlocale(LC_ALL, "C");

  CHECK(FLAGS_http_port < 65536);
  if (FLAGS_threads <= 0) {
    FLAGS_threads = folly::hardware_concurrency();
    CHECK(FLAGS_threads > 0);
  }

  std::vector<HTTPServer::IPConfig> IPs;
  for (const std::string &bindTo : {FLAGS_http_if1, FLAGS_http_if2, FLAGS_http_if3}) {
    if (!bindTo.empty()) {
      IPs.emplace_back(folly::SocketAddress{bindTo, FLAGS_http_port, true},
                       HTTPServer::Protocol::HTTP);
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
  auto db = loadMappingFile(argv[1]);
  options.handlerFactories = RequestHandlerChain()
    .addThen(makeApiHandlerFactory(db))
    .build();
  HTTPServer server(std::move(options));

  LOG(INFO) << "Starting HTTP server on port " << FLAGS_http_port;
  server.bind(IPs);
  server.start();

  return 0;
}
