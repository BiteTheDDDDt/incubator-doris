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
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/arithmeticOverflow.h
// and modified by Doris

#pragma once

#include "vec/core/wide_integer.h"
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wconversion"
#endif

namespace common {
template <typename T>
inline bool add_overflow(T x, T y, T& res) {
    return __builtin_add_overflow(x, y, &res);
}

template <>
inline bool add_overflow(int x, int y, int& res) {
    return __builtin_sadd_overflow(x, y, &res);
}

template <>
inline bool add_overflow(long x, long y, long& res) {
    return __builtin_saddl_overflow(x, y, &res);
}

template <>
inline bool add_overflow(long long x, long long y, long long& res) {
    return __builtin_saddll_overflow(x, y, &res);
}

template <>
inline bool add_overflow(__int128 x, __int128 y, __int128& res) {
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 =
            (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x + y;
    return (y > 0 && x > max_int128 - y) || (y < 0 && x < min_int128 - y);
}

template <>
inline bool add_overflow(wide::Int256 x, wide::Int256 y, wide::Int256& res) {
    static constexpr wide::Int256 min_int256 = std::numeric_limits<wide::Int256>::min();
    static constexpr wide::Int256 max_int256 = std::numeric_limits<wide::Int256>::max();
    res = x + y;
    return (y > 0 && x > max_int256 - y) || (y < 0 && x < min_int256 - y);
}
template <typename T>
inline bool sub_overflow(T x, T y, T& res) {
    return __builtin_sub_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(int x, int y, int& res) {
    return __builtin_ssub_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(long x, long y, long& res) {
    return __builtin_ssubl_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(long long x, long long y, long long& res) {
    return __builtin_ssubll_overflow(x, y, &res);
}

template <>
inline bool sub_overflow(__int128 x, __int128 y, __int128& res) {
    static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
    static constexpr __int128 max_int128 =
            (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
    res = x - y;
    return (y < 0 && x > max_int128 + y) || (y > 0 && x < min_int128 + y);
}

template <>
inline bool sub_overflow(wide::Int256 x, wide::Int256 y, wide::Int256& res) {
    static constexpr wide::Int256 min_int256 = std::numeric_limits<wide::Int256>::min();
    static constexpr wide::Int256 max_int256 = std::numeric_limits<wide::Int256>::max();
    res = x - y;
    return (y < 0 && x > max_int256 + y) || (y > 0 && x < min_int256 + y);
}

template <typename T>
inline bool mul_overflow(T x, T y, T& res) {
    return __builtin_mul_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(int x, int y, int& res) {
    return __builtin_smul_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(long x, long y, long& res) {
    return __builtin_smull_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(long long x, long long y, long long& res) {
    return __builtin_smulll_overflow(x, y, &res);
}

template <>
inline bool mul_overflow(__int128 x, __int128 y, __int128& res) {
    res = static_cast<unsigned __int128>(x) *
          static_cast<unsigned __int128>(y); /// Avoid signed integer overflow.
    if (!x || !y) return false;

    unsigned __int128 a = (x > 0) ? x : -x;
    unsigned __int128 b = (y > 0) ? y : -y;
    return (a * b) / b != a;
}

template <>
inline bool mul_overflow(wide::Int256 x, wide::Int256 y, wide::Int256& res) {
    res = static_cast<wide::UInt256>(x) * static_cast<wide::UInt256>(y);
    if (!x || !y) return false;
    wide::UInt256 a = (x > 0) ? x : -x;
    wide::UInt256 b = (y > 0) ? y : -y;
    return (a * b) / b != a;
}
} // namespace common
#ifdef __clang__
#pragma clang diagnostic pop
#endif