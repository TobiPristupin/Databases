#ifndef DATAINTENSIVE_DBMEMCACHE_H
#define DATAINTENSIVE_DBMEMCACHE_H

#include <string>
#include "../DatabaseEntry.h"
#include "MemCache.h"


using DbMemCache = MemCache<std::string, DbValue>;


#endif
