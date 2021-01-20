#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fstream>
#include <functional>
#include <thread>
#include <atomic>
#include <systemd/sd-daemon.h>
#include <systemd/sd-journal.h>
#include <glog/logging.h>
#include <folly/stop_watch.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/system/ThreadId.h>
#include <folly/String.h>
#include <folly/Format.h>

#include "CallFwd.h"
#include "PhoneMapping.h"
#include "ACL.h"

static std::atomic<PhoneMapping::Data*> currentMapping;
static std::atomic<ACL::Data*> currentACL;
static constexpr auto reportInterval = std::chrono::seconds(30);

PhoneMapping PhoneMapping::get() noexcept { return { currentMapping }; }
bool PhoneMapping::isAvailable() noexcept { return !!currentMapping.load(); }
ACL ACL::get() noexcept { return { currentACL }; }

static folly::StringPiece osBasename(folly::StringPiece path) {
  auto idx = path.rfind('/');
  if (idx == folly::StringPiece::npos) {
    return path.str();
  }
  return path.subpiece(idx + 1);
}

static bool loadACLFile(int fd) {
  std::string path = folly::sformat("/proc/self/fd/{}", fd);
  std::unique_ptr<ACL::Data> data;
  std::ifstream in;
  size_t line = 0;

  try {
    in.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    in.open(path);
    data = ACL::fromCSV(in, line);
    in.close();
  } catch (std::exception& e) {
    LOG(ERROR) << osBasename(path) << ':' << line << ": " << e.what();
    return false;
  }

  LOG(INFO) << "Replacing ACL (" << line << " rows)...";
  ACL::commit(std::move(data), currentACL);
  folly::hazptr_cleanup();
  return true;
}

static bool loadMappingFile(std::string path, size_t estimate)
{
  std::ifstream in;
  std::vector<char> rbuf(1ull << 19);
  folly::stop_watch<> watch;
  PhoneMapping::Builder builder;
  size_t nrows = 0;

  try {
    in.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    in.rdbuf()->pubsetbuf(rbuf.data(), rbuf.size());
    in.open(path);

    builder.sizeHint(estimate + estimate / 20);
    LOG(INFO) << "Reading database from " << osBasename(path)
      << " (" << estimate << " rows estimated)";

    while (in.good()) {
      builder.fromCSV(in, nrows, 10000);
      if (watch.lap(reportInterval)) {
        LOG_IF(INFO, estimate != 0) << nrows * 100 / estimate << "% completed";
        LOG_IF(INFO, estimate == 0) << nrows << " rows read";
      }
    }
    in.close();
  } catch (std::runtime_error &e) {
    LOG(ERROR) << osBasename(path) << ':' << nrows << ": " << e.what();
    return false;
  }

  LOG(INFO) << "Building index (" << nrows << " rows)...";
  builder.commit(currentMapping);
  folly::hazptr_cleanup();
  return true;
}

static bool loadMappingFile(int fd)
{
  struct stat s;
  memset(&s, 0, sizeof(s));

  size_t lineEstimate = 0;
  if (fstat(fd, &s) == 0 && s.st_size > 1000)
    lineEstimate = s.st_size / 23;

  std::string path = folly::sformat("/proc/self/fd/{}", fd);
  return loadMappingFile(path, lineEstimate);
}

static bool loadMappingFile(const char* fname)
{
  struct stat s;
  memset(&s, 0, sizeof(s));

  size_t lineEstimate = 0;
  if (lstat(fname, &s) == 0 && s.st_size > 1000)
    lineEstimate = s.st_size / 23;

  return loadMappingFile(fname, lineEstimate);
}

static bool verifyMappingFile(int fd)
{
  std::string path = folly::sformat("/proc/self/fd/{}", fd);
  std::ifstream in;
  std::string linebuf;
  std::vector<uint64_t> row;
  folly::stop_watch<> watch;
  size_t maxdiff = 100;
  size_t nrows = 0;
  PhoneMapping db = PhoneMapping::get();

  try {
    LOG(INFO) << "Verifying database";
    in.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    in.open(path);
    while (in.good()) {
      for (size_t i = 0; i < 10000; ++i) {
        if (in.peek() == EOF)
          break;

        row.clear();
        std::getline(in, linebuf);
        folly::split(',', linebuf, row);
        ++nrows;

        if (db.getRN(row[0]) == row[1])
          continue;

        LOG(ERROR) << osBasename(path) << ":" << nrows
                    << ": key " << row[0] << " differs";
        if (--maxdiff == 0) {
          LOG(ERROR) << "Diff limit reached, stopping";
          return false;
        }
      }
      if (watch.lap(reportInterval))
        LOG_IF(INFO, db.size()) << nrows * 100 / db.size() << "% completed";
    }
    in.close();
  } catch (std::runtime_error& e) {
    LOG(ERROR) << osBasename(path) << ":" << nrows << ": " << e.what();
    return false;
  }

  if (nrows != db.size()) {
    LOG(ERROR) << "Loaded DB has " << db.size() - nrows << " extra rows";
    return false;
  }

  LOG(INFO) << "Loaded database matches file";
  return true;
}

static bool dumpMappingFile(int fd)
{
  std::string path = folly::sformat("/proc/self/fd/{}", fd);
  std::ofstream out;

  PhoneMapping db = PhoneMapping::get();
  folly::stop_watch<> watch;
  size_t nrows = 0;

  try {
    LOG(INFO) << "Dumping database";
    out.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    out.open(path);

    for (db.visitRows(); db.hasRow(); ) {
      for (size_t i = 0; i < 10000 && db.hasRow(); ++i) {
        out << db.currentPN() << "," << db.currentRN() << "\r\n";
        db.advance();
        ++nrows;
      }
      if (watch.lap(reportInterval))
        LOG_IF(INFO, db.size()) << nrows * 100 / db.size() << "% completed";
    }
    out.flush();
    out.close();
  } catch (std::runtime_error& e) {
    LOG(ERROR) << osBasename(path) << ":" << nrows << ": " << e.what();
    return false;
  }

  LOG(INFO) << nrows << " rows dumped";
  return true;
}

class FdLogSink : public google::LogSink {
public:
  FdLogSink(int fd)
    : fd_(fd)
  {
  }

  ~FdLogSink() override = default;

  void send(google::LogSeverity severity, const char* full_filename,
            const char* base_filename, int line,
            const struct ::tm* tm_time,
            const char* message, size_t message_len) override
  {
    std::string buf = ToString(severity, base_filename, line,
                               tm_time, message, message_len);
    buf += '\n';
    write(fd_, buf.data(), buf.size());
  }

private:
  int fd_;
};

class JournaldSink : public google::LogSink {
public:
  void send(google::LogSeverity severity, const char* full_filename,
            const char* base_filename, int line,
            const struct ::tm* tm_time,
            const char* message, size_t message_len) override
  {
    // This array maps Google severity levels to syslog levels
    const int SEVERITY_TO_LEVEL[] = { LOG_INFO, LOG_WARNING, LOG_ERR, LOG_EMERG };

    sd_journal_send("MESSAGE=%.*s", int(message_len), message,
                    "PRIORITY=%i", SEVERITY_TO_LEVEL[static_cast<int>(severity)],
                    "TID=%i", int(folly::getOSThreadID()),
                    "CODE_FILE=%s", full_filename,
                    "CODE_LINE=%d", line,
                    NULL);
  }
};

static void controlThread(int sock, const char *initialDB) {
  loadMappingFile(initialDB);

  std::string pkt(1500, 'x');
  union {
      char buf[256];
      struct cmsghdr align;
  } u;

  struct iovec iov{pkt.data(), pkt.length()};
  struct msghdr msg;
  struct cmsghdr *c;
  struct sockaddr_un peer;

  while (true) {
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = &peer;
    msg.msg_namelen = sizeof(peer);
    msg.msg_flags = 0;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    ssize_t ret = recvmsg(sock, &msg, MSG_CMSG_CLOEXEC);
    if (ret <= 0) {
      LOG(WARNING) << "Control socket error";
      continue;
    }

    folly::StringPiece cmd{pkt.data(), (size_t)ret};
    std::vector<int> fds;

    for (c = CMSG_FIRSTHDR(&msg); c != NULL; c = CMSG_NXTHDR(&msg, c)) {
      if (c->cmsg_len == 0)
        continue;
      if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS) {
        fds.resize((c->cmsg_len - sizeof(struct cmsghdr)) / sizeof(int));
        memcpy(fds.data(), CMSG_DATA(c), fds.size()*sizeof(int));
        break;
      }
    }

    std::unique_ptr<google::LogSink> stderr;

    if (fds.size() > 0) {
      stderr.reset(new FdLogSink(fds[0]));
      google::AddLogSink(stderr.get());
    }

    char success = 'F';
    if (cmd == "LOAD_DB" && fds.size() == 2) {
      if (loadMappingFile(fds[1]))
        success = 'S';
    } else if (cmd == "VERIFY_DB" && fds.size() == 2) {
      if (verifyMappingFile(fds[1]))
        success = 'S';
    } else if (cmd == "DUMP_DB" && fds.size() == 2) {
      if (dumpMappingFile(fds[1]))
        success = 'S';
    } else if (cmd == "LOAD_ACL" && fds.size() == 2) {
      if (loadACLFile(fds[1]))
        success = 'S';
    } else {
      LOG(WARNING) << "Unrecognized command: " << cmd << "(fds: " << fds.size() << ")";
    }

    if (msg.msg_namelen > 0) {
      if (sendto(sock, &success, 1, 0, (struct sockaddr*)&peer, msg.msg_namelen) < 0)
        PLOG(WARNING) << "sendto";
    }

    google::RemoveLogSink(stderr.get());
    for (int fd : fds)
      close(fd);
  }
}

void startControlSocket(const char* initialDB) {
  static JournaldSink journalSink;
  if (sd_listen_fds(0) != 1) {
    LOG(WARNING) << "launched without systemd, control socket disabled";
    return;
  }

  // Replace default log sinks with systemd
  google::AddLogSink(&journalSink);
  for ( int i = 0; i < google::NUM_SEVERITIES; ++i ) {
    google::SetLogDestination(i, "");     // "" turns off logging to a logfile
  }
  google::SetStderrLogging(google::FATAL);

  int fd = SD_LISTEN_FDS_START + 0;
  std::thread(std::bind(controlThread, fd, initialDB))
     .detach();
}
