#include "remote_slot.h"

#include <atomic>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <immintrin.h>
#include <mutex>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "epoch.h"
#include "log.h"
#include "node_config.h"

namespace smallbank {

static constexpr uint32_t kMagic = 0x5342564c; // 'SBVL'

struct SignalMsg {
  uint32_t magic;
  int32_t origin_node;
  uint64_t epoch_nr;
  uint64_t offset;
  uint64_t sid;
  int64_t value;
};

struct Peer {
  int fd = -1;
  std::mutex mu;
};

static Peer g_peers[felis::kMaxNrNode];

static bool ReadFull(int fd, void *buf, size_t n)
{
  auto *p = (uint8_t *) buf;
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::read(fd, p + got, n - got);
    if (r == 0) return false;
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    got += r;
  }
  return true;
}

static bool WriteFull(int fd, const void *buf, size_t n)
{
  auto *p = (const uint8_t *) buf;
  size_t put = 0;
  while (put < n) {
    ssize_t r = ::send(fd, p + put, n - put, MSG_NOSIGNAL);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    put += r;
  }
  return true;
}

static void Apply(const SignalMsg &msg)
{
  auto &mgr = util::Instance<felis::EpochManager>();
  if (msg.magic != kMagic) return;
  if (msg.epoch_nr != mgr.current_epoch_nr()) return;
  uint8_t *base = mgr.ptr(msg.epoch_nr, msg.origin_node, 0);
  auto *sidp = (uint64_t *) (base + msg.offset);
  auto *valp = (int64_t *) (base + msg.offset + sizeof(uint64_t));
  __atomic_store_n(valp, msg.value, __ATOMIC_RELAXED);
  __atomic_store_n(sidp, msg.sid, __ATOMIC_RELEASE);
}

static void Reader(int fd)
{
  for (;;) {
    SignalMsg msg;
    if (!ReadFull(fd, &msg, sizeof(msg))) return;
    Apply(msg);
  }
}

static void Tune(int fd)
{
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

static int ConnectRetry(const std::string &host, uint16_t port)
{
  for (int attempt = 0; attempt < 120; attempt++) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo *res = nullptr;
    int ga = getaddrinfo(host.c_str(), nullptr, &hints, &res);
    if (ga != 0 || res == nullptr) {
      sleep(1);
      continue;
    }
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
      freeaddrinfo(res);
      sleep(1);
      continue;
    }
    sockaddr_in addr{};
    memcpy(&addr, res->ai_addr, sizeof(addr));
    addr.sin_port = htons(port);
    freeaddrinfo(res);

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int rc = ::connect(fd, (sockaddr *) &addr, sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
      close(fd);
      sleep(1);
      continue;
    }
    pollfd pfd{fd, POLLOUT, 0};
    int pr = poll(&pfd, 1, 1000);
    int err = 0;
    socklen_t len = sizeof(err);
    if (pr == 1) getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (pr == 1 && err == 0) {
      Tune(fd);
      return fd;
    }
    close(fd);
  }
  return -1;
}

static int ListenOn(uint16_t port)
{
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  abort_if(fd < 0, "smallbank signal socket failed errno {}", errno);
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  abort_if(bind(fd, (sockaddr *) &addr, sizeof(addr)) < 0,
           "smallbank signal bind {} failed errno {}", port, errno);
  abort_if(listen(fd, 16) < 0, "smallbank signal listen failed errno {}", errno);
  return fd;
}

static void RegisterPeer(int peer, int fd)
{
  abort_if(peer <= 0 || peer >= felis::kMaxNrNode, "smallbank peer {}", peer);
  std::lock_guard<std::mutex> guard(g_peers[peer].mu);
  g_peers[peer].fd = fd;
  std::thread(Reader, fd).detach();
}

void StartSignalBus()
{
  auto &conf = util::Instance<felis::NodeConfiguration>();
  int me = conf.node_id();
  int nr = conf.nr_nodes();
  if (nr <= 1) return;

  uint16_t port = conf.config().worker_peer.port + 1;
  int lfd = ListenOn(port);
  int expect = me - 1;
  std::atomic<int> accepted{0};

  std::thread acceptor([lfd, expect, &accepted] {
    for (int i = 0; i < expect; i++) {
      int fd = ::accept(lfd, nullptr, nullptr);
      abort_if(fd < 0, "smallbank signal accept failed errno {}", errno);
      Tune(fd);
      int32_t peer = 0;
      abort_if(!ReadFull(fd, &peer, sizeof(peer)), "smallbank signal hello failed");
      RegisterPeer(peer, fd);
      accepted.fetch_add(1);
    }
  });

  for (int peer = me + 1; peer <= nr; peer++) {
    auto host = conf.config(peer).worker_peer.host;
    uint16_t peer_port = conf.config(peer).worker_peer.port + 1;
    int fd = ConnectRetry(host, peer_port);
    abort_if(fd < 0, "smallbank cannot connect to node {} {}:{}", peer, host, peer_port);
    int32_t hello = me;
    abort_if(!WriteFull(fd, &hello, sizeof(hello)), "smallbank hello to {} failed", peer);
    RegisterPeer(peer, fd);
    logger->info("smallbank signal connected to node {}", peer);
  }

  while (accepted.load() < expect)
    usleep(1000);
  acceptor.join();
  logger->info("smallbank signal bus ready on port {}", port);
}

void PublishRemote(int dest_node, int origin_node, uint64_t epoch_nr,
                   uint64_t offset, uint64_t sid, int64_t value)
{
  SignalMsg msg{kMagic, origin_node, epoch_nr, offset, sid, value};
  auto &peer = g_peers[dest_node];
  std::lock_guard<std::mutex> guard(peer.mu);
  abort_if(peer.fd < 0, "smallbank signal peer {} is not connected", dest_node);
  abort_if(!WriteFull(peer.fd, &msg, sizeof(msg)),
           "smallbank signal send to {} failed errno {}", dest_node, errno);
}

int64_t WaitRemote(int origin_node, uint64_t epoch_nr, uint64_t offset, uint64_t sid)
{
  auto &mgr = util::Instance<felis::EpochManager>();
  uint8_t *base = mgr.ptr(epoch_nr, origin_node, 0);
  auto *sidp = (uint64_t *) (base + offset);
  auto *valp = (int64_t *) (base + offset + sizeof(uint64_t));
  long spins = 0;
  while (__atomic_load_n(sidp, __ATOMIC_ACQUIRE) != sid) {
    spins++;
    if ((spins & 0x0FFFF) == 0) {
      auto routine = go::Scheduler::Current()->current_routine();
      if (((felis::BasePieceCollection::ExecutionRoutine *) routine)->Preempt(true))
        continue;
    }
    _mm_pause();
  }
  return __atomic_load_n(valp, __ATOMIC_RELAXED);
}

}
