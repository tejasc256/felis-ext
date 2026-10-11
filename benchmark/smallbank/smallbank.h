#ifndef SMALLBANK_H
#define SMALLBANK_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>

#include "table_decl.h"
#include "epoch.h"
#include "slice.h"
#include "index.h"
#include "txn_cc.h"
#include "log.h"
#include "util/random.h"

namespace smallbank {

// Balances are integer cents. OLTP-Bench's dollar amounts (1.3, 5.0, 20.2, 5.0)
// are 130, 500, 2020, and 500. Initial balances are uniform from $10,000 to $50,000.
static constexpr int64_t kMinBalance = 10000 * 100;
static constexpr int64_t kMaxBalance = 50000 * 100;
static constexpr int64_t kDepositChecking = 130;
static constexpr int64_t kSendPayment = 500;
static constexpr int64_t kTransactSavings = 2020;
static constexpr int64_t kWriteCheck = 500;
// Classic SmallBank penalty is $1 when savings + checking is short.
static constexpr int64_t kCheckPenalty = 100;

enum class TableType : int {
  SmallBankBase = 300,
  Account,
  Savings,
  Checking,
};

enum class TxnKind : int {
  Amalgamate = 0,
  Balance,
  DepositChecking,
  SendPayment,
  TransactSavings,
  WriteCheck,
};

struct SliceRouter {
  static int SliceToNodeId(int16_t slice_id) { return slice_id + 1; }
};

inline int16_t CustomerSlice(uint32_t cust_id) {
  return cust_id % util::Instance<felis::NodeConfiguration>().nr_nodes();
}

inline int CustomerNode(uint32_t cust_id) {
  return SliceRouter::SliceToNodeId(CustomerSlice(cust_id));
}

struct Account {
  static uint32_t HashKey(const felis::VarStrView &k) {
    uint32_t be;
    memcpy(&be, k.data(), sizeof(be));
    return be32toh(be);
  }
  static constexpr auto kTable = TableType::Account;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, size_t(1 << 20), false);
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::SmallBankKey;
  using Value = sql::SmallBankAccountValue;
};

struct Savings {
  static uint32_t HashKey(const felis::VarStrView &k) { return Account::HashKey(k); }
  static constexpr auto kTable = TableType::Savings;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, size_t(1 << 20), false);
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::SmallBankKey;
  using Value = sql::SmallBankBalanceValue;
};

struct Checking {
  static uint32_t HashKey(const felis::VarStrView &k) { return Account::HashKey(k); }
  static constexpr auto kTable = TableType::Checking;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, size_t(1 << 20), false);
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::SmallBankKey;
  using Value = sql::SmallBankBalanceValue;
};

enum RowSlot : int {
  kAccount0 = 0,
  kSavings0,
  kChecking0,
  kAccount1,
  kSavings1,
  kChecking1,
  kRowSlots,
};

struct SBState {
  // remote_value must sit immediately after remote_sid. The signal bus
  // writes both from the offset of remote_sid.
  uint64_t remote_sid;
  int64_t remote_value;
  felis::VHandle *rows[kRowSlots];

  struct SlotArg {
    int slot;
    int is_write;
    size_t EncodeSize() const { return sizeof(SlotArg); }
    uint8_t *EncodeTo(uint8_t *buf) const {
      memcpy(buf, this, sizeof(SlotArg));
      return buf + sizeof(SlotArg);
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      memcpy(this, buf, sizeof(SlotArg));
      return buf + sizeof(SlotArg);
    }
  };

  struct LookupCompletion : public felis::TxnStateCompletion<SBState> {
    SlotArg args;
    void operator()(int id, felis::BaseTxn::LookupRowResult found) {
      abort_if(found[0] == nullptr, "smallbank missing row slot {}", args.slot);
      state->rows[args.slot] = found[0];
      if (args.is_write)
        handle(found[0]).AppendNewVersion();
    }
  };
};

static_assert(offsetof(SBState, remote_value) == offsetof(SBState, remote_sid) + sizeof(uint64_t),
              "signal bus packs the value after the serial id");

struct Input {
  TxnKind kind;
  uint32_t c0;
  uint32_t c1;
};

class Client : public felis::EpochClient {
  util::FastRandom rng;
 public:
  static size_t g_num_accounts;

  Client();
  unsigned int LoadPercentage() final override { return 100; }
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;

  uint32_t Pick();
  uint32_t PickOther(uint32_t other);
  Input GenerateInput();
  // Local mix when this node generates its own load. The routed list otherwise.
  Input TakeInput();
};

// Blocks until the submitter has sent this node's transactions.
void AcceptRoutedTxns();

class Loader : public go::Routine {
  std::atomic_bool done{false};
 public:
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

}

namespace felis {

using namespace smallbank;

SHARD_TABLE(Account) { return CustomerSlice(key.cust_id); }
SHARD_TABLE(Savings) { return CustomerSlice(key.cust_id); }
SHARD_TABLE(Checking) { return CustomerSlice(key.cust_id); }

}

#endif
