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

#pragma once

#include "vec/common/unaligned.h"
#include "vec/core/wide_integer.h"

inline uint64_t gbswap_64(uint64_t host_int) {
#if defined(__GNUC__) && defined(__x86_64__) && !defined(__APPLE__)
    // Adapted from /usr/include/byteswap.h.  Not available on Mac.
    if (__builtin_constant_p(host_int)) {
        return __bswap_constant_64(host_int);
    } else {
        uint64_t result;
        __asm__("bswap %0" : "=r"(result) : "0"(host_int));
        return result;
    }
#elif defined(bswap_64)
    return bswap_64(host_int);
#else
    return static_cast<uint64_t>(bswap_32(static_cast<uint32>(host_int >> 32))) |
           (static_cast<uint64_t>(bswap_32(static_cast<uint32>(host_int))) << 32);
#endif // bswap_64
}

inline unsigned __int128 gbswap_128(unsigned __int128 host_int) {
    return static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int >> 64))) |
           (static_cast<unsigned __int128>(bswap_64(static_cast<uint64_t>(host_int))) << 64);
}

inline wide::UInt256 gbswap_256(wide::UInt256 host_int) {
    wide::UInt256 result {gbswap_64(host_int.items[3]), gbswap_64(host_int.items[2]),
                          gbswap_64(host_int.items[1]), gbswap_64(host_int.items[0])};
    return result;
}

// Swap bytes of a 24-bit value.
inline uint32_t bswap_24(uint32_t x) {
    return ((x & 0x0000ffULL) << 16) | ((x & 0x00ff00ULL)) | ((x & 0xff0000ULL) >> 16);
}

// Utilities to convert numbers between the current hosts's native byte
// order and little-endian byte order
//
// Load/Store methods are alignment safe
class LittleEndian {
public:
    // Conversion functions.
#if __BYTE_ORDER == __LITTLE_ENDIAN

    static uint16_t FromHost16(uint16_t x) { return x; }
    static uint16_t ToHost16(uint16_t x) { return x; }

    static uint32_t FromHost32(uint32_t x) { return x; }
    static uint32_t ToHost32(uint32_t x) { return x; }

    static uint64_t FromHost64(uint64_t x) { return x; }
    static uint64_t ToHost64(uint64_t x) { return x; }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return x; }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return x; }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return x; }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return x; }

#elif __BYTE_ORDER == __BIG_ENDIAN

    static uint16_t FromHost16(uint16_t x) { return bswap_16(x); }
    static uint16_t ToHost16(uint16_t x) { return bswap_16(x); }

    static uint32_t FromHost32(uint32_t x) { return bswap_32(x); }
    static uint32_t ToHost32(uint32_t x) { return bswap_32(x); }

    static uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    static uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return gbswap_128(x); }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return gbswap_128(x); }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return gbswap_256(x); }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return gbswap_256(x); }

#endif /* ENDIAN */

    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) { return ToHost16(unaligned_load<uint16_t>(p)); }

    static void Store16(void* p, uint16_t v) { unaligned_store<uint16_t>(p, FromHost16(v)); }

    static uint32_t Load32(const void* p) { return ToHost32(unaligned_load<uint32_t>(p)); }

    static void Store32(void* p, uint32_t v) { unaligned_store<uint32_t>(p, FromHost32(v)); }

    static uint64_t Load64(const void* p) { return ToHost64(unaligned_load<uint64_t>(p)); }

    static void Store64(void* p, uint64_t v) { unaligned_store<uint64_t>(p, FromHost64(v)); }
};

// Utilities to convert numbers between the current hosts's native byte
// order and big-endian byte order (same as network byte order)
//
// Load/Store methods are alignment safe
class BigEndian {
public:
#if __BYTE_ORDER == __LITTLE_ENDIAN

    static uint16_t FromHost16(uint16_t x) { return bswap_16(x); }
    static uint16_t ToHost16(uint16_t x) { return bswap_16(x); }

    static uint32_t FromHost24(uint32_t x) { return bswap_24(x); }
    static uint32_t ToHost24(uint32_t x) { return bswap_24(x); }

    static uint32_t FromHost32(uint32_t x) { return bswap_32(x); }
    static uint32_t ToHost32(uint32_t x) { return bswap_32(x); }

    static uint64_t FromHost64(uint64_t x) { return gbswap_64(x); }
    static uint64_t ToHost64(uint64_t x) { return gbswap_64(x); }

    static unsigned __int128 FromHost128(unsigned __int128 x) { return gbswap_128(x); }
    static unsigned __int128 ToHost128(unsigned __int128 x) { return gbswap_128(x); }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return gbswap_256(x); }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return gbswap_256(x); }

#elif __BYTE_ORDER == __BIG_ENDIAN

    static uint16_t FromHost16(uint16_t x) { return x; }
    static uint16_t ToHost16(uint16_t x) { return x; }

    static uint32_t FromHost24(uint32_t x) { return x; }
    static uint32_t ToHost24(uint32_t x) { return x; }

    static uint32_t FromHost32(uint32_t x) { return x; }
    static uint32_t ToHost32(uint32_t x) { return x; }

    static uint64_t FromHost64(uint64_t x) { return x; }
    static uint64_t ToHost64(uint64_t x) { return x; }

    static wide::UInt256 FromHost256(wide::UInt256 x) { return x; }
    static wide::UInt256 ToHost256(wide::UInt256 x) { return x; }

#endif /* ENDIAN */
    // Functions to do unaligned loads and stores in little-endian order.
    static uint16_t Load16(const void* p) { return ToHost16(unaligned_load<uint16_t>(p)); }

    static void Store16(void* p, uint16_t v) { unaligned_store<uint16_t>(p, FromHost16(v)); }

    static uint32_t Load32(const void* p) { return ToHost32(unaligned_load<uint32_t>(p)); }

    static void Store32(void* p, uint32_t v) { unaligned_store<uint32_t>(p, FromHost32(v)); }

    static uint64_t Load64(const void* p) { return ToHost64(unaligned_load<uint64_t>(p)); }

    static void Store64(void* p, uint64_t v) { unaligned_store<uint64_t>(p, FromHost64(v)); }
};
