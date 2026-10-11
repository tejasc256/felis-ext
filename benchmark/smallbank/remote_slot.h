#ifndef SMALLBANK_REMOTE_SLOT_H
#define SMALLBANK_REMOTE_SLOT_H

#include <cstdint>

namespace smallbank {

// Cross-node pieces cannot share epoch state. PublishRemote writes a
// (serial id, value) pair into the destination node's replica of the
// origin epoch object. WaitRemote spins until that serial id appears.
void StartSignalBus();
void PublishRemote(int dest_node, int origin_node, uint64_t epoch_nr,
                   uint64_t offset, uint64_t sid, int64_t value);
int64_t WaitRemote(int origin_node, uint64_t epoch_nr, uint64_t offset,
                   uint64_t sid);

}

#endif
