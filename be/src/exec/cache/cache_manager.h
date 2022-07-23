// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once
#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "util/slice.h"
`
namespace starrocks {
namespace cache {
class CacheManager;
using CacheManagerRawPtr = CacheManager*;
using CacheManagerPtr = std::shared_ptr<CacheManager>;

using CacheResult = std::vector<vectorized::ChunkPtr>;

struct CacheKey {
    Slice prefix;
    int64_t tablet_id;
};

struct CacheValue {
    int64_t latest_hit_time;
    int64_t hit_count;
    int64_t populate_time;
    int64_t version;
    CacheResult result;
};

class CacheManager {
public:
    Status populate(const CacheKey& key, const CacheValue& value);
    StatusOr<CacheValue> probe(const CacheKey& key);
};
} // namespace cache
} // namespace starrocks
