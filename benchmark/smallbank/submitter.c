/* One process generates every SmallBank transaction and sends each one to
 * the server that owns its first customer. Servers reply when the run ends.
 *
 *   sb-submit <accounts> <epochs> <epoch-size> <host:port> [host:port ...]
 *
 * epoch-size is the number of transactions each server runs per epoch.
 * Total transactions = epoch-size * epochs * number of servers.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum {
  kAmalgamate = 0,
  kSendPayment = 3,
  kWriteCheck = 5
};

static uint32_t g_rng = 0x5B11BA;

static uint32_t rnd(void)
{
  g_rng ^= g_rng << 13;
  g_rng ^= g_rng >> 17;
  g_rng ^= g_rng << 5;
  return g_rng;
}

static void write_full(int fd, const void *buf, size_t n)
{
  const uint8_t *p = buf;
  size_t put = 0;
  while (put < n) {
    ssize_t w = write(fd, p + put, n - put);
    if (w <= 0) {
      perror("write");
      exit(1);
    }
    put += w;
  }
}

static void read_full(int fd, void *buf, size_t n)
{
  uint8_t *p = buf;
  size_t got = 0;
  while (got < n) {
    ssize_t r = read(fd, p + got, n - got);
    if (r <= 0) {
      perror("read");
      exit(1);
    }
    got += r;
  }
}

static int connect_retry(const char *host, int port)
{
  for (int attempt = 0; attempt < 120; attempt++) {
    struct addrinfo hints, *res = NULL;
    char port_s[16];
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(port_s, sizeof(port_s), "%d", port);
    if (getaddrinfo(host, port_s, &hints, &res) != 0) {
      sleep(1);
      continue;
    }
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd >= 0 && connect(fd, res->ai_addr, res->ai_addrlen) == 0) {
      freeaddrinfo(res);
      return fd;
    }
    if (fd >= 0)
      close(fd);
    freeaddrinfo(res);
    sleep(1);
  }
  fprintf(stderr, "could not connect to %s:%d\n", host, port);
  exit(1);
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    fprintf(stderr, "usage: %s <accounts> <epochs> <epoch-size> <host:port>...\n", argv[0]);
    return 1;
  }
  uint32_t accounts = (uint32_t) strtoul(argv[1], NULL, 10);
  uint32_t epochs = (uint32_t) strtoul(argv[2], NULL, 10);
  uint32_t epoch_size = (uint32_t) strtoul(argv[3], NULL, 10);
  int nservers = argc - 4;
  if (accounts < 2 || epochs == 0 || epoch_size == 0 || nservers < 1) {
    fprintf(stderr, "bad arguments\n");
    return 1;
  }

  uint32_t count = epochs * epoch_size;
  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);

  int *fds = calloc(nservers, sizeof(int));
  for (int s = 0; s < nservers; s++) {
    char *spec = strdup(argv[4 + s]);
    char *colon = strchr(spec, ':');
    if (!colon) {
      fprintf(stderr, "expected host:port, got %s\n", spec);
      return 1;
    }
    *colon = 0;
    int port = atoi(colon + 1);
    fds[s] = connect_retry(spec, port);

    struct { uint32_t magic, count; } hdr = {0x53425231, count};
    write_full(fds[s], &hdr, sizeof(hdr));

    /* Each server runs a full epoch. The first customer is forced onto that
     * server, matching CustomerSlice = cust_id % nr_nodes. */
    for (uint32_t n = 0; n < count; n++) {
      static const int mix[] = {15, 15, 15, 25, 15, 15};
      int rd = rnd() % 100;
      int kind = 0;
      for (; kind < 5; kind++) {
        if (rd < mix[kind])
          break;
        rd -= mix[kind];
      }
      uint32_t c0;
      do {
        c0 = rnd() % accounts;
      } while ((c0 % (uint32_t) nservers) != (uint32_t) s);
      uint32_t c1 = 0;
      if (kind == kAmalgamate || kind == kSendPayment) {
        do {
          c1 = rnd() % accounts;
        } while (c1 == c0);
      }
      struct { uint32_t kind, c0, c1; } rec = {(uint32_t) kind, c0, c1};
      write_full(fds[s], &rec, sizeof(rec));
    }
    fprintf(stderr, "sent %u transactions to %s:%d\n", count, spec, port);
    free(spec);
  }

  uint64_t total_txn = (uint64_t) count * (uint64_t) nservers;
  long sum_thr = 0;
  for (int s = 0; s < nservers; s++) {
    struct {
      uint32_t magic, throughput, insert_ms, init_ms, exec_ms;
    } ack;
    read_full(fds[s], &ack, sizeof(ack));
    if (ack.magic != 0x53424B31) {
      fprintf(stderr, "bad ack from server %d\n", s + 1);
      return 1;
    }
    printf("server %d throughput %u txn/s insert %u ms initialize %u ms execute %u ms\n",
           s + 1, ack.throughput, ack.insert_ms, ack.init_ms, ack.exec_ms);
    sum_thr += ack.throughput;
    close(fds[s]);
  }
  clock_gettime(CLOCK_MONOTONIC, &t1);
  double e2e_ms = (t1.tv_sec - t0.tv_sec) * 1000.0
      + (t1.tv_nsec - t0.tv_nsec) / 1e6;
  printf("transactions %llu\n", (unsigned long long) total_txn);
  printf("cluster_throughput %ld txn/s\n", sum_thr);
  printf("submit_to_complete_ms %.3f\n", e2e_ms);
  printf("e2e_throughput %.0f txn/s\n", total_txn * 1000.0 / e2e_ms);
  free(fds);
  return 0;
}
