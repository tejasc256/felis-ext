// -*- mode: c++ -*-

#ifndef SMALLBANK_TABLE_DECL_H
#define SMALLBANK_TABLE_DECL_H

#include "sqltypes.h"

namespace sql {

FIELD(uint32_t, cust_id);
KEYS(SmallBankKey);

FIELD(int64_t, cust_id);
VALUES(SmallBankAccountValue);

FIELD(int64_t, bal);
VALUES(SmallBankBalanceValue);

}

#endif
