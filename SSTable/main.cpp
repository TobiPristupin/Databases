#include <iostream>
#include "MemCache.h"
#include "SSTableDb.h"
#include "Workload.h"
/*
 * Ideas for benchmarking
 *
 * 1) Workload of random read, writes, deletes
 *  - Record number of reads/writes/deletes, and percentiles for each
 *
 * 2) Comparison of reads in memcache vs reads in SSFile
 * - Fill the memcache with 1024 keys, and then flush the memcache, moving everything to a SSFile. Perform the
 *  same sequence of reads to each and determine the time difference.
 */


int main(){
//    SSTableDb db(std::make_unique<);
//    std::unique_ptr<DbMemCache> memCache = std::make_unique<AvlTree<std::string, DbValue>>();
//    memCache->insert("hola", 123.1);
//    memCache->insert("caca", INT64_MAX);
//    memCache->insert("llavelarga", 1.1);
//    memCache->insert("llavelarga1", 1.2);
//    memCache->insert("llavelarga2", 1.3);
//    memCache->insert("llavelarga3", 1.4);
//    memCache->insert("llavelarga4", 1.5);
//    memCache->insert("1", "cacorruti");
//    auto ssfile = SSFileCreator::newFile(std::filesystem::path{"."}, 1, memCache.get(), {});
//    std::cout << dbValueToString(ssfile->get("1").value()) << "\n";

    unsigned long seed = 1;
    WorkloadGenerator workloadGenerator(seed);
    auto workload = workloadGenerator.generateRandomWorkload(50, 5);
    return 0;
}