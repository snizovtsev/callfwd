#include <fstream>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/portability/GFlags.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "ApiHandler.h"
#include "PhoneMapping.h"


using namespace proxygen;

using folly::SocketAddress;

using Protocol = HTTPServer::Protocol;

DEFINE_int32(http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_string(listen, "localhost", "IP/Hostname to bind to");
DEFINE_string(listen1, "", "Additional address to bind to");
DEFINE_string(listen2, "", "Additional address to bind to");
DEFINE_string(listen3, "", "Additional address to bind to");
DEFINE_int32(threads,
             0,
             "Number of threads to listen on. Numbers <= 0 "
             "will use the number of cores on this machine.");


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

  auto db = loadMappingFile(argv[1]);

  std::vector<HTTPServer::IPConfig> IPs = {
    {SocketAddress(FLAGS_listen, FLAGS_http_port, true), Protocol::HTTP},
  };
  if (!FLAGS_listen1.empty())
    IPs.push_back({SocketAddress(FLAGS_listen1, FLAGS_http_port, true), Protocol::HTTP});
  if (!FLAGS_listen2.empty())
    IPs.push_back({SocketAddress(FLAGS_listen2, FLAGS_http_port, true), Protocol::HTTP});
  if (!FLAGS_listen3.empty())
    IPs.push_back({SocketAddress(FLAGS_listen3, FLAGS_http_port, true), Protocol::HTTP});

  if (FLAGS_threads <= 0) {
    FLAGS_threads = folly::hardware_concurrency();
    CHECK(FLAGS_threads > 0);
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
    .addThen(std::move(makeApiHandlerFactory(db)))
    .build();
  HTTPServer server(std::move(options));

  LOG(INFO) << "Starting HTTP server on port " << FLAGS_http_port;
  server.bind(IPs);
  server.start();

  return 0;
}
