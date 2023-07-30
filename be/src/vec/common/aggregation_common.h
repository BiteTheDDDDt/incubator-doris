// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/AggregationCommon.h
// and modified by Doris

#pragma once

#include <array>

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"

namespace doris::vectorized {

using Sizes = std::vector<size_t>;

/// When packing the values of nullable columns at a given row, we have to
/// store the fact that these values are nullable or not. This is achieved
/// by encoding this information as a bitmap. Let S be the size in bytes of
/// a packed values binary blob and T the number of bytes we may place into
/// this blob, the size that the bitmap shall occupy in the blob is equal to:
/// ceil(T/8). Thus we must have: S = T + ceil(T/8). Below we indicate for
/// each value of S, the corresponding value of T, and the bitmap size:
///
/// 32,28,4
/// 16,14,2
/// 8,7,1
/// 4,3,1
/// 2,1,1
///

namespace {
// clang-format off
template <typename T>
constexpr auto get_bitmap_size() {
    return (sizeof(T) == 32)
            ? 4: (sizeof(T) == 16)
            ? 2: ((sizeof(T) == 8)
            ? 1: ((sizeof(T) == 4) 
            ? 1: ((sizeof(T) == 2) 
            ? 1: 0)));
}
// clang-format on

} // namespace

template <typename T>
using KeysNullMap = std::array<UInt8, get_bitmap_size<T>()>;

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T>
T pack_fixed(size_t i, size_t keys_size, const ColumnRawPtrs& key_columns, const Sizes& key_sizes) {
    union {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    for (size_t j = 0; j < keys_size; ++j) {
        size_t index = i;
        const IColumn* column = key_columns[j];

        switch (key_sizes[j]) {
        case 1:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(column)->get_raw_data_begin<1>() + index,
                   1);
            offset += 1;
            break;
        case 2:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(column)->get_raw_data_begin<2>() +
                           index * 2,
                   2);
            offset += 2;
            break;
        case 4:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(column)->get_raw_data_begin<4>() +
                           index * 4,
                   4);
            offset += 4;
            break;
        case 8:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(column)->get_raw_data_begin<8>() +
                           index * 8,
                   8);
            offset += 8;
            break;
        default:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(column)->get_raw_data_begin<1>() +
                           index * key_sizes[j],
                   key_sizes[j]);
            offset += key_sizes[j];
        }
    }

    return key;
}

/// Similar as above but supports nullable values.
template <typename T>
T pack_fixed(size_t i, size_t keys_size, const ColumnRawPtrs& key_columns, const Sizes& key_sizes,
             const KeysNullMap<T>& bitmap) {
    union {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    static constexpr auto bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
    static constexpr bool has_bitmap = bitmap_size > 0;

    if (has_bitmap) {
        memcpy(bytes + offset, bitmap.data(), bitmap_size * sizeof(UInt8));
        offset += bitmap_size;
    }

    for (size_t j = 0; j < keys_size; ++j) {
        bool is_null = false;

        if (has_bitmap) {
            size_t bucket = j / 8;
            size_t off = j % 8;
            is_null = ((bitmap[bucket] >> off) & 1) == 1;
        }

        if (is_null) {
            offset += key_sizes[j];
            continue;
        }

        switch (key_sizes[j]) {
        case 1:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(key_columns[j])->get_raw_data_begin<1>() +
                           i,
                   1);
            break;
        case 2:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(key_columns[j])->get_raw_data_begin<2>() +
                           i * 2,
                   2);
            break;
        case 4:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(key_columns[j])->get_raw_data_begin<4>() +
                           i * 4,
                   4);
            break;
        case 8:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(key_columns[j])->get_raw_data_begin<8>() +
                           i * 8,
                   8);
            break;
        default:
            memcpy(bytes + offset,
                   static_cast<const ColumnVectorHelper*>(key_columns[j])->get_raw_data_begin<1>() +
                           i * key_sizes[j],
                   key_sizes[j]);
            offset += key_sizes[j];
        }

        offset += key_sizes[j];
    }

    return key;
}

template <typename T>
std::vector<T> pack_fixeds(size_t row_numbers, const ColumnRawPtrs& key_columns,
                           const Sizes& key_sizes, const ColumnRawPtrs& nullmap_columns) {
    size_t bitmap_size = 0;
    if (!nullmap_columns.empty()) {
        bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
    }

    std::vector<T> result(row_numbers);
    size_t offset = 0;
    if (bitmap_size > 0) {
        for (size_t j = 0; j < nullmap_columns.size(); j++) {
            if (!nullmap_columns[j]) {
                continue;
            }
            size_t bucket = j / 8;
            size_t offset = j % 8;
            const auto& data =
                    assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
            for (size_t i = 0; i < row_numbers; ++i) {
                *((char*)(&result[i]) + bucket) |= data[i] << offset;
            }
        }
        offset += bitmap_size;
    }

    // make sure null cell is filled by 0x0
    T zero {};
    for (size_t j = 0; j < key_columns.size(); ++j) {
        const char* data = key_columns[j]->get_raw_data().data;

        if (key_sizes[j] == 1) {
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_1((char*)(&result[i]) + offset,
                                   nullmap[i] ? (char*)&zero : data + i * key_sizes[j]);
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_1((char*)(&result[i]) + offset, data + i * key_sizes[j]);
                }
            }

        } else if (key_sizes[j] == 2) {
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_2((char*)(&result[i]) + offset,
                                   nullmap[i] ? (char*)&zero : data + i * key_sizes[j]);
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_2((char*)(&result[i]) + offset, data + i * key_sizes[j]);
                }
            }
        } else if (key_sizes[j] == 4) {
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_4((char*)(&result[i]) + offset,
                                   nullmap[i] ? (char*)&zero : data + i * key_sizes[j]);
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_4((char*)(&result[i]) + offset, data + i * key_sizes[j]);
                }
            }
        } else if (key_sizes[j] == 8) {
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_8((char*)(&result[i]) + offset,
                                   nullmap[i] ? (char*)&zero : data + i * key_sizes[j]);
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_8((char*)(&result[i]) + offset, data + i * key_sizes[j]);
                }
            }
        } else if (key_sizes[j] == 16) {
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_16((char*)(&result[i]) + offset,
                                    nullmap[i] ? (char*)&zero : data + i * key_sizes[j]);
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed_16((char*)(&result[i]) + offset, data + i * key_sizes[j]);
                }
            }
        } else {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "pack_fixeds input invalid key size, key_size={}", key_sizes[j]);
        }
        offset += key_sizes[j];
    }
    return result;
}

/// Hash a set of keys into a UInt128 value.
inline UInt128 hash128(size_t i, size_t keys_size, const ColumnRawPtrs& key_columns) {
    UInt128 key;
    SipHash hash;

    for (size_t j = 0; j < keys_size; ++j) {
        key_columns[j]->update_hash_with_value(i, hash);
    }

    hash.get128(key.low, key.high);

    return key;
}

/// Copy keys to the pool. Then put into pool StringRefs to them and return the pointer to the first.
inline StringRef* place_keys_in_pool(size_t keys_size, StringRefs& keys, Arena& pool) {
    for (size_t j = 0; j < keys_size; ++j) {
        char* place = pool.alloc(keys[j].size);
        memcpy_small_allow_read_write_overflow15(place, keys[j].data, keys[j].size);
        keys[j].data = place;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    char* res = pool.aligned_alloc(keys_size * sizeof(StringRef), alignof(StringRef));
    memcpy_small_allow_read_write_overflow15(res, keys.data(), keys_size * sizeof(StringRef));

    return reinterpret_cast<StringRef*>(res);
}

/** Serialize keys into a continuous chunk of memory.
  */
inline StringRef serialize_keys_to_pool_contiguous(size_t i, size_t keys_size,
                                                   const ColumnRawPtrs& key_columns, Arena& pool) {
    const char* begin = nullptr;

    size_t sum_size = 0;
    for (size_t j = 0; j < keys_size; ++j) {
        sum_size += key_columns[j]->serialize_value_into_arena(i, pool, begin).size;
    }

    return {begin, sum_size};
}

} // namespace doris::vectorized
