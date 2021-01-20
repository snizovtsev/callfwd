#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <thread>
#include <iomanip>
#include <atomic>
#include <systemd/sd-daemon.h>
#include <systemd/sd-journal.h>
#include <glog/logging.h>
#include <folly/stop_watch.h>
#include <folly/Synchronized.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/system/ThreadId.h>
#include <folly/String.h>
#include <folly/Conv.h>
#include <folly/TokenBucket.h>
#include <folly/IPAddress.h>
#include <proxygen/lib/utils/Time.h>

#include "CallFwd.h"
#include "PhoneMapping.h"

using proxygen::SystemTimePoint;

struct ACLRule {
  SystemTimePoint created;
  folly::Optional<SystemTimePoint> expire;
  folly::TokenBucket callLimiter{1e10, 1e10};
};

static std::atomic<PhoneMapping::Data*> currentMapping;
static folly::Synchronized<std::shared_ptr<std::unordered_map<folly::IPAddress, ACLRule>>> ACLs;
static constexpr auto reportInterval = std::chrono::seconds(30);

PhoneMapping PhoneMapping::get() noexcept
{
  return {currentMapping};
}

bool PhoneMapping::isAvailable() noexcept {
  return !!currentMapping.load();
}

int checkACL(const folly::IPAddress &peer)
{
  auto acls = ACLs.copy();
  if (!acls) return 401;

  auto it = acls->find(peer);
  if (it == acls->cend())
    return 401;

  auto& rule = it->second;

  if (rule.expire && SystemTimePoint::clock::now() >= rule.expire.value())
    return 401;

  if (rule.callLimiter.consume(1))
    return 200;
  else
    return 429;
}

static bool loadACLFile(int fd)
{
  using TAclMap =std::unordered_map<folly::IPAddress, ACLRule>;
  TAclMap acl;
  std::string line;
  std::vector<std::string> row;
  size_t nrows = 0;

  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ifstream in(fname);

  while (getline(in, line)) {
    try {
      folly::splitTo<std::string>(",", line, std::back_inserter(row));
      folly::IPAddress ip(row[0]);

      acl[ip] = {};

      if (!row[1].empty()) {
        std::tm tm = {};
        std::stringstream ss(row[1]);
        //2015-10-09 08:00:00+00
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail())
          throw std::runtime_error("failed parsing created_on time");
        acl[ip].created = SystemTimePoint::clock::from_time_t(std::mktime(&tm));
      }

      if (!row[2].empty()) {
        std::tm tm = {};
        std::stringstream ss(row[2]);
        //2015-10-09 08:00:00+00
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail())
          throw std::runtime_error("failed parsing expire time");
        acl[ip].expire = SystemTimePoint::clock::from_time_t(std::mktime(&tm));
        //std::cerr << acl[ip].expire.value() << std::endl;
      }

      if (auto cps = folly::tryTo<double>(row[3]))
        acl[ip].callLimiter.reset(cps.value(), std::max(cps.value(), 1.));
    } catch (std::exception& e) {
      LOG(ERROR) << "ACLRead failed on line " << nrows << ": " << e.what();
      in.close();
      return false;
    }
    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "ACLRead failed on line " << nrows;
    return false;
  }
  in.close();

  LOG(INFO) << "Replacing ACL (" << nrows << " rows)...";
  ACLs.exchange(std::make_shared<TAclMap>(std::move(acl)));
  return true;
}

static bool loadMappingFile(std::ifstream &in, PhoneMapping::Builder &builder, size_t est)
{
  std::vector<char> rbuf(1ull << 19);
  std::string line;
  std::vector<uint64_t> rowbuf;
  folly::stop_watch<> watch;
  size_t nrows = 0;

  in.rdbuf()->pubsetbuf(rbuf.data(), rbuf.size());
  while (getline(in, line)) try {
    if ((nrows & 0xfff) == 0xfff && watch.lap(reportInterval)) {
      if (est != 0) {
        LOG(INFO) << nrows * 100 / est << "% completed";
      } else {
        LOG(INFO) << nrows << " rows read";
      }
    }

    folly::splitTo<uint64_t>(",", line, std::back_inserter(rowbuf));
    builder.addRow(rowbuf[0], rowbuf[1]);
    rowbuf.clear();
    ++nrows;
  } catch (std::runtime_error& e) {
    LOG(ERROR) << "Read failed on line " << nrows << ": " << e.what();
    in.close();
    return false;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "Read failed on line " << nrows;
    return false;
  }
  in.close();

  LOG(INFO) << "Building index (" << nrows << " rows)...";
  builder.commit(currentMapping);
  folly::hazptr_cleanup();
  return true;
}

static bool loadMappingFile(int fd)
{
  PhoneMapping::Builder builder;

  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ifstream in(fname);

  CHECK(!in.fail());

  struct stat s;
  memset(&s, 0, sizeof(s));

  size_t lineEstimate = 0;
  if (fstat(fd, &s) == 0) {
    if (s.st_size > 1000) {
      lineEstimate = s.st_size / 23;
      builder.sizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }

  LOG(INFO) << "Reloading database";
  return loadMappingFile(in, builder, lineEstimate);
}

static bool loadMappingFile(const char* fname)
{
  PhoneMapping::Builder builder;
  std::ifstream in(fname);
  CHECK(!in.fail()) << "Failed to open " << fname;

  struct stat s;
  memset(&s, 0, sizeof(s));

  size_t lineEstimate = 0;
  if (lstat(fname, &s) == 0) {
    if (s.st_size > 1000) {
      lineEstimate = s.st_size / 23;
      builder.sizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }

  LOG(INFO) << "Reading database from " << fname << " ...";
  return loadMappingFile(in, builder, lineEstimate);
}

static bool verifyMappingFile(int fd)
{
  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ifstream in(fname);

  CHECK(!in.fail());

  LOG(INFO) << "Verifying database";
  PhoneMapping db = PhoneMapping::get();
  std::string line;
  std::vector<uint64_t> row;
  folly::stop_watch<> watch;
  size_t nrows = 0;
  size_t maxdiff = 100;

  while (getline(in, line)) try {
    if ((nrows & 0xfff) == 0xfff && watch.lap(reportInterval)) {
      if (db.size() != 0) {
        LOG(INFO) << nrows * 100 / db.size() << "% completed";
      }
    }

    folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
    if (db.getRN(row[0]) != row[1]) {
      LOG(ERROR) << (nrows+1) << ": key " << row[0] << " differs";
      if (--maxdiff == 0) {
        LOG(ERROR) << "Diff limit reached, stopping";
        in.close();
        return false;
      }
    }
    row.clear();
    ++nrows;
  } catch (std::runtime_error& e) {
    LOG(ERROR) << "Read failed on line " << nrows << ": " << e.what();
    in.close();
    return false;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "Read failed on line " << nrows;
    return false;
  }

  if (nrows != db.size()) {
    LOG(ERROR) << "Loaded DB has " << db.size() - nrows << " extra rows";
    return false;
  }

  in.close();
  LOG(INFO) << "Loaded database matches file";
  return true;
}

static bool dumpMappingFile(int fd)
{
  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ofstream out(fname);

  LOG(INFO) << "Dumping database";
  folly::stop_watch<> watch;
  size_t nrows = 0;

  for (PhoneMapping db = PhoneMapping::get().visitRows();
       out.good() && db.hasRow(); db.advance())
    {
      if ((nrows & 0xfff) == 0xfff && watch.lap(reportInterval)) {
        if (db.size() != 0) {
          LOG(INFO) << nrows * 100 / db.size() << "% completed";
        }
      }
      out << db.currentPN() << "," << db.currentRN() << "\r\n";
      ++nrows;
    }

  out.flush();
  if (out.fail() || out.bad()) {
    out.close();
    LOG(ERROR) << "Write failed on line " << nrows;
    return false;
  }

  out.close();

  LOG(INFO) << "Dump completed";
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

  google::AddLogSink(&journalSink);
  for ( int i = 0; i < google::NUM_SEVERITIES; ++i ) {
    google::SetLogDestination(i, "");     // "" turns off logging to a logfile
  }

  int fd = SD_LISTEN_FDS_START + 0;
  std::thread(std::bind(controlThread, fd, initialDB))
     .detach();
}
