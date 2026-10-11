#include "smallbank.h"
#include "remote_slot.h"

#include <atomic>
#include <cstdlib>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "log.h"
#include "node_config.h"
#include "opts.h"
#include "util/os.h"

namespace smallbank {

using namespace felis;

static std::atomic<int64_t> g_balance_sink{0};

size_t Client::g_num_accounts = 100000;

static int64_t InitialBalance(util::FastRandom &rng)
{
  uint32_t span = (uint32_t) (kMaxBalance - kMinBalance + 1);
  return kMinBalance + (int64_t) (rng.next_u32() % span);
}

Client::Client()
    : rng(0x5B11BA ^ util::Instance<NodeConfiguration>().node_id())
{}

uint32_t Client::Pick()
{
  return rng.next_u32() % g_num_accounts;
}

uint32_t Client::PickOther(uint32_t other)
{
  uint32_t id;
  do {
    id = Pick();
  } while (id == other);
  return id;
}

Input Client::GenerateInput()
{
  static constexpr int kMix[] = {15, 15, 15, 25, 15, 15};
  int rd = rng.next_u32() % 100;
  int kind = 0;
  for (; kind < 5; kind++) {
    if (rd < kMix[kind]) break;
    rd -= kMix[kind];
  }
  Input in;
  in.kind = TxnKind(kind);
  in.c0 = Pick();
  in.c1 = 0;
  if (in.kind == TxnKind::Amalgamate || in.kind == TxnKind::SendPayment)
    in.c1 = PickOther(in.c0);
  return in;
}

static std::vector<Input> g_routed;
static size_t g_routed_at = 0;
static int g_router_fd = -1;

Input Client::TakeInput()
{
  if (g_routed.empty())
    return GenerateInput();
  abort_if(g_routed_at >= g_routed.size(),
           "routed transaction list ran out at {}", g_routed_at);
  return g_routed[g_routed_at++];
}

static void ReadFull(int fd, void *buf, size_t n)
{
  auto *p = static_cast<uint8_t *>(buf);
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::read(fd, p + got, n - got);
    abort_if(r <= 0, "routed submitter closed the connection");
    got += r;
  }
}

static void WriteFull(int fd, const void *buf, size_t n)
{
  auto *p = static_cast<const uint8_t *>(buf);
  size_t put = 0;
  while (put < n) {
    ssize_t w = ::write(fd, p + put, n - put);
    abort_if(w <= 0, "routed submitter ack write failed");
    put += w;
  }
}

static void OnRoutedRunFinished(long throughput, long insert_ms, long init_ms,
                                long exec_ms)
{
  if (g_router_fd < 0)
    return;
  struct Ack {
    uint32_t magic;
    uint32_t throughput;
    uint32_t insert_ms;
    uint32_t init_ms;
    uint32_t exec_ms;
  } ack;
  ack.magic = 0x53424B31;
  ack.throughput = (uint32_t) throughput;
  ack.insert_ms = (uint32_t) insert_ms;
  ack.init_ms = (uint32_t) init_ms;
  ack.exec_ms = (uint32_t) exec_ms;
  WriteFull(g_router_fd, &ack, sizeof(ack));
  ::close(g_router_fd);
  g_router_fd = -1;
}

void AcceptRoutedTxns()
{
  if (!Options::kSmallBankRouted)
    return;

  auto &conf = util::Instance<NodeConfiguration>();
  int port = conf.config().worker_peer.port + 2;
  int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  abort_if(listen_fd < 0, "routed listen socket failed");
  int yes = 1;
  ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  abort_if(::bind(listen_fd, (sockaddr *) &addr, sizeof(addr)) < 0,
           "routed bind {} failed", port);
  abort_if(::listen(listen_fd, 1) < 0, "routed listen failed");
  logger->info("smallbank waiting for routed transactions on {}", port);

  int fd = ::accept(listen_fd, nullptr, nullptr);
  abort_if(fd < 0, "routed accept failed");
  ::close(listen_fd);

  struct Hdr {
    uint32_t magic;
    uint32_t count;
  } hdr;
  ReadFull(fd, &hdr, sizeof(hdr));
  abort_if(hdr.magic != 0x53425231, "routed batch has a bad header");

  size_t expect = EpochClient::g_txn_per_epoch * (EpochClient::g_max_epoch - 1);
  abort_if(hdr.count != expect,
           "routed batch has {} transactions, this node expects {}",
           hdr.count, expect);

  g_routed.resize(hdr.count);
  for (uint32_t i = 0; i < hdr.count; i++) {
    struct Rec {
      uint32_t kind;
      uint32_t c0;
      uint32_t c1;
    } rec;
    ReadFull(fd, &rec, sizeof(rec));
    abort_if(rec.kind > (uint32_t) TxnKind::WriteCheck, "routed kind {}", rec.kind);
    abort_if(rec.c0 >= Client::g_num_accounts ||
             (rec.c1 != 0 && rec.c1 >= Client::g_num_accounts),
             "routed customer out of range");
    g_routed[i].kind = TxnKind(rec.kind);
    g_routed[i].c0 = rec.c0;
    g_routed[i].c1 = rec.c1;
  }

  g_router_fd = fd;
  EpochClient::g_run_finished_hook = OnRoutedRunFinished;
  logger->info("smallbank accepted {} routed transactions", hdr.count);
}

class SBTxn : public Txn<SBState>, public Input {
 public:
  SBTxn(Client *client, uint64_t serial_id)
      : Txn<SBState>(serial_id), Input(client->TakeInput()) {}

  void PrepareInsert() override final {}
  void Prepare() override final;
  void Run() override final;

  template <typename Table>
  void Lookup(int slot, bool is_write, uint32_t cust) {
    typename Table::Key key;
    key.cust_id = cust;
    SBState::SlotArg arg{slot, is_write ? 1 : 0};
    TxnIndexLookup<SliceRouter, SBState::LookupCompletion, SBState::SlotArg>(
        &arg, KeyParam<Table>(key));
  }
};

void SBTxn::Prepare()
{
  INIT_ROUTINE_BRK(8192);
  switch (kind) {
    case TxnKind::Amalgamate:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Account>(kAccount1, false, c1);
      Lookup<Savings>(kSavings0, true, c0);
      Lookup<Checking>(kChecking0, true, c0);
      Lookup<Checking>(kChecking1, true, c1);
      break;
    case TxnKind::Balance:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Savings>(kSavings0, false, c0);
      Lookup<Checking>(kChecking0, false, c0);
      break;
    case TxnKind::DepositChecking:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Checking>(kChecking0, true, c0);
      break;
    case TxnKind::SendPayment:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Account>(kAccount1, false, c1);
      Lookup<Checking>(kChecking0, true, c0);
      Lookup<Checking>(kChecking1, true, c1);
      break;
    case TxnKind::TransactSavings:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Savings>(kSavings0, true, c0);
      break;
    case TxnKind::WriteCheck:
      Lookup<Account>(kAccount0, false, c0);
      Lookup<Savings>(kSavings0, false, c0);
      Lookup<Checking>(kChecking0, true, c0);
      break;
  }
}

static uint64_t SlotOffset(const GenericEpochObject<SBState> &st)
{
  return st.buffer_offset() + offsetof(SBState, remote_sid);
}

static void ReadAccount(Txn<SBState>::TxnHandle index_handle, VHandle *row)
{
  index_handle(row).Read<Account::Value>();
}

static int64_t ReadBalance(Txn<SBState>::TxnHandle index_handle, VHandle *row)
{
  return index_handle(row).Read<Savings::Value>().bal;
}

static void WriteBalance(Txn<SBState>::TxnHandle index_handle, VHandle *row, int64_t bal)
{
  Savings::Value v;
  v.bal = bal;
  index_handle(row).Write(v);
}

static void RunPiece(const GenericEpochObject<SBState> &state,
                     Txn<SBState>::TxnHandle index_handle,
                     int kind, uint32_t c0, uint32_t c1, int side)
{
  auto row = [&](int slot) { return state->rows[slot]; };
  int64_t sid = index_handle.serial_id();

  if (kind == (int) TxnKind::Balance) {
    ReadAccount(index_handle, row(kAccount0));
    int64_t total = ReadBalance(index_handle, row(kSavings0))
        + ReadBalance(index_handle, row(kChecking0));
    g_balance_sink.fetch_add(total, std::memory_order_relaxed);
    return;
  }

  if (kind == (int) TxnKind::DepositChecking) {
    ReadAccount(index_handle, row(kAccount0));
    WriteBalance(index_handle, row(kChecking0),
                 ReadBalance(index_handle, row(kChecking0)) + kDepositChecking);
    return;
  }

  if (kind == (int) TxnKind::TransactSavings) {
    ReadAccount(index_handle, row(kAccount0));
    int64_t bal = ReadBalance(index_handle, row(kSavings0));
    if (bal >= kTransactSavings) bal -= kTransactSavings;
    WriteBalance(index_handle, row(kSavings0), bal);
    return;
  }

  if (kind == (int) TxnKind::WriteCheck) {
    ReadAccount(index_handle, row(kAccount0));
    int64_t savings = ReadBalance(index_handle, row(kSavings0));
    int64_t checking = ReadBalance(index_handle, row(kChecking0));
    int64_t debit = kWriteCheck;
    if (savings + checking < debit) debit += kCheckPenalty;
    WriteBalance(index_handle, row(kChecking0), checking - debit);
    return;
  }

  if (kind == (int) TxnKind::Amalgamate) {
    if (side == 0 || side == 1) {
      ReadAccount(index_handle, row(kAccount0));
      int64_t sum = ReadBalance(index_handle, row(kSavings0))
          + ReadBalance(index_handle, row(kChecking0));
      WriteBalance(index_handle, row(kSavings0), 0);
      WriteBalance(index_handle, row(kChecking0), 0);
      if (side == 1) {
        PublishRemote(CustomerNode(c1), state.origin_node_id(), state.nr(),
                      SlotOffset(state), sid, sum);
        return;
      }
      ReadAccount(index_handle, row(kAccount1));
      WriteBalance(index_handle, row(kChecking1),
                   ReadBalance(index_handle, row(kChecking1)) + sum);
      return;
    }
    int64_t sum = WaitRemote(state.origin_node_id(), state.nr(), SlotOffset(state), sid);
    ReadAccount(index_handle, row(kAccount1));
    WriteBalance(index_handle, row(kChecking1),
                 ReadBalance(index_handle, row(kChecking1)) + sum);
    return;
  }

  // SendPayment. side 0 is both customers, side 1 is the sender, side 2 is the destination.
  if (side == 0 || side == 1) {
    ReadAccount(index_handle, row(kAccount0));
    int64_t bal = ReadBalance(index_handle, row(kChecking0));
    int64_t delta = 0;
    if (bal >= kSendPayment) {
      bal -= kSendPayment;
      delta = kSendPayment;
    }
    WriteBalance(index_handle, row(kChecking0), bal);
    if (side == 1) {
      PublishRemote(CustomerNode(c1), state.origin_node_id(), state.nr(),
                    SlotOffset(state), sid, delta);
      return;
    }
    ReadAccount(index_handle, row(kAccount1));
    WriteBalance(index_handle, row(kChecking1),
                 ReadBalance(index_handle, row(kChecking1)) + delta);
    return;
  }
  int64_t delta = WaitRemote(state.origin_node_id(), state.nr(), SlotOffset(state), sid);
  ReadAccount(index_handle, row(kAccount1));
  WriteBalance(index_handle, row(kChecking1),
               ReadBalance(index_handle, row(kChecking1)) + delta);
}

void SBTxn::Run()
{
  int n0 = CustomerNode(c0);
  bool two = kind == TxnKind::Amalgamate || kind == TxnKind::SendPayment;
  int n1 = two ? CustomerNode(c1) : n0;

  auto attach = [this](int node, int side) {
    root->AttachRoutine(
        MakeContext((int) kind, c0, c1, side), node,
        [](const auto &ctx) {
          auto &[state, index_handle, kind, c0, c1, side] = ctx;
          RunPiece(state, index_handle, kind, c0, c1, side);
        });
  };

  if (n0 == n1) {
    attach(n0, 0);
  } else {
    attach(n0, 1);
    attach(n1, 2);
  }
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new SBTxn(this, serial_id);
}

void Loader::Run()
{
  auto &mgr = util::Instance<TableManager>();
  mgr.Create<Account, Savings, Checking>();

  auto &conf = util::Instance<NodeConfiguration>();
  int node = conf.node_id();
  int nr_nodes = conf.nr_nodes();
  auto nr_threads = NodeConfiguration::g_nr_threads;
  void *buf = alloca(64);
  util::FastRandom rng(0xA11CE ^ node);

  for (auto t = 0; t < nr_threads; t++) {
    MasstreeIndex::ResetThreadInfo();
    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    uint32_t seq = 0;
    for (uint32_t id = node - 1; id < Client::g_num_accounts; id += nr_nodes, seq++) {
      if ((seq % nr_threads) != (uint32_t) t) continue;

      Account::Key key;
      key.cust_id = id;

      Account::Value account;
      account.cust_id = id;
      auto account_row = mgr.Get<Account>().SearchOrCreate(key.EncodeView(buf));
      felis::InitVersion(account_row, account.Encode());

      int64_t bal = InitialBalance(rng);
      Savings::Value savings;
      savings.bal = bal;
      auto savings_row = mgr.Get<Savings>().SearchOrCreate(key.EncodeView(buf));
      felis::InitVersion(savings_row, savings.Encode());

      Checking::Value checking;
      checking.bal = InitialBalance(rng);
      auto checking_row = mgr.Get<Checking>().SearchOrCreate(key.EncodeView(buf));
      felis::InitVersion(checking_row, checking.Encode());
    }
  }

  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();
  mem::ParallelPool::SetCurrentAffinity(-1);
  MasstreeIndex::ResetThreadInfo();

  logger->info("smallbank node {} loaded its share of {} accounts across {} nodes",
               node, Client::g_num_accounts, nr_nodes);
  done = true;
}

}
