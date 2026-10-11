#include "smallbank.h"
#include "remote_slot.h"
#include "module.h"
#include "opts.h"

namespace felis {

class SmallBankModule : public Module<WorkloadModule> {
 public:
  SmallBankModule() {
    info = {
      .name = "smallbank",
      .description = "SmallBank",
    };
  }
  void Init() override final {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    if (Options::kSmallBankAccounts)
      smallbank::Client::g_num_accounts = Options::kSmallBankAccounts.ToLargeNumber();
    abort_if(smallbank::Client::g_num_accounts < 2,
             "smallbank needs at least 2 accounts, got {}",
             smallbank::Client::g_num_accounts);

    smallbank::StartSignalBus();

    auto loader = new smallbank::Loader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    // Data is loaded. The submitter is the only source of transactions.
    smallbank::AcceptRoutedTxns();

    EpochClient::g_workload_client = new smallbank::Client();
  }
};

static SmallBankModule smallbank_module;

}
