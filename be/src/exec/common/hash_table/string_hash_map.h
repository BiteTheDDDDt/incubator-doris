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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/StringHashMap.h
// and modified by Doris

#pragma once

#include <parallel_hashmap/phmap.h>

#include <boost/noncopyable.hpp>

#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exec/common/hash_table/string_hash_table.h"

namespace doris {

/// A cell type that stores a key-mapped pair and provides the interface
/// expected by StringHashTableEmpty and StringHashTable's outer iterator.
/// This replaces the old HashMapCell/StringHashMapCell hierarchy.
///
/// For non-StringRef keys (T1-T5), the cell stores the fixed-size key and mapped value.
/// For StringRef key (T0, Ts), the cell stores a StringRef key and mapped value.
///
/// The critical interface requirements:
///   - .value.first / .value.second (used by StringHashTableEmpty)
///   - get_second(), get_mapped(), get_key() (used by iterator, callables)
///   - Convertible from cells with other key types (for outer iterator)
template <typename Key, typename TMapped>
struct StringHashMapCell {
    using Mapped = TMapped;
    using mapped_type = Mapped;
    using key_type = Key;

    struct ValuePair {
        Key first {};
        Mapped second {};
    };
    using value_type = ValuePair;

    ValuePair value;

    StringHashMapCell() = default;

    const Key& get_first() const { return value.first; }
    Mapped& get_second() { return value.second; }
    const Mapped& get_second() const { return value.second; }

    Mapped& get_mapped() { return value.second; }
    const Mapped& get_mapped() const { return value.second; }

    static const Key& get_key(const ValuePair& v) { return v.first; }

    // For StringHashTable outer iterator: convert to StringRef
    doris::StringRef get_key() const { return to_string_ref(value.first); }

    // Allow construction/assignment from cells with different key types
    // (used by the outer iterator to convert to the common Ts cell type)
    template <typename OtherKey>
    StringHashMapCell(const StringHashMapCell<OtherKey, TMapped>& other) {
        value.first = other.get_key();
        value.second = other.get_second();
    }

    template <typename OtherKey>
    StringHashMapCell& operator=(const StringHashMapCell<OtherKey, TMapped>& other) {
        value.first = other.get_key();
        value.second = other.get_second();
        return *this;
    }
};

/// Specialization for StringRef key — get_key() returns StringRef directly.
template <typename TMapped>
struct StringHashMapCell<doris::StringRef, TMapped> {
    using Key = doris::StringRef;
    using Mapped = TMapped;
    using mapped_type = Mapped;
    using key_type = Key;

    struct ValuePair {
        Key first {};
        Mapped second {};
    };
    using value_type = ValuePair;

    ValuePair value;

    StringHashMapCell() = default;

    /// Two-argument constructor: used by StringHashTableEmpty::Constructor
    /// when the creator lambda calls ctor(key, mapped).
    StringHashMapCell(const Key& key, const Mapped& mapped) {
        value.first = key;
        value.second = mapped;
    }

    /// Single-argument constructor: used when ctor(key) is called (set-like usage).
    explicit StringHashMapCell(const Key& key) {
        value.first = key;
        value.second = Mapped {};
    }

    const Key& get_first() const { return value.first; }
    Mapped& get_second() { return value.second; }
    const Mapped& get_second() const { return value.second; }

    Mapped& get_mapped() { return value.second; }
    const Mapped& get_mapped() const { return value.second; }

    static const Key& get_key(const ValuePair& v) { return v.first; }
    const doris::StringRef& get_key() const { return value.first; }

    // Allow construction/assignment from cells with any key type
    template <typename OtherKey>
    StringHashMapCell(const StringHashMapCell<OtherKey, TMapped>& other) {
        value.first = other.get_key();
        value.second = other.get_second();
    }

    template <typename OtherKey>
    StringHashMapCell& operator=(const StringHashMapCell<OtherKey, TMapped>& other) {
        value.first = other.get_key();
        value.second = other.get_second();
        return *this;
    }
};

/// A phmap-based sub-map for StringHashMap, providing the same interface
/// that StringHashTable and hash_map_context.h expect from sub-maps.
/// Replaces HashMapTable<Key, StringHashMapCell<Key, TMapped>, ...>.
template <typename Key, typename TMapped, typename Hash = StringHashTableHash>
class StringSubMap : private boost::noncopyable {
public:
    using Self = StringSubMap;
    using Mapped = TMapped;
    using HashMapImpl = doris::flat_hash_map<Key, Mapped, Hash>;
    using value_type = typename HashMapImpl::value_type;
    using mapped_type = Mapped;
    using key_type = Key;
    using cell_type = StringHashMapCell<Key, Mapped>;

    // LookupResult: a pointer to the phmap entry that can be null-checked
    // and provides ->get_second() / ->get_mapped().
    // We use the phmap's std::pair<const Key, Mapped>* but wrap access
    // through a thin result type.
    struct LookupResultImpl {
        using pair_type = std::pair<const Key, Mapped>;
        pair_type* ptr = nullptr;

        LookupResultImpl() = default;
        explicit LookupResultImpl(pair_type* p) : ptr(p) {}

        Mapped& get_second() { return ptr->second; }
        const Mapped& get_second() const { return ptr->second; }
        Mapped& get_mapped() { return ptr->second; }
        const Mapped& get_mapped() const { return ptr->second; }
        const Key& get_first() const { return ptr->first; }

        explicit operator bool() const { return ptr != nullptr; }
        auto* operator->() { return this; }
        const auto* operator->() const { return this; }
        auto& operator*() { return *this; }

        friend bool operator==(const LookupResultImpl& a, std::nullptr_t) { return !a.ptr; }
        friend bool operator!=(const LookupResultImpl& a, std::nullptr_t) {
            return a.ptr != nullptr;
        }
        friend bool operator==(std::nullptr_t, const LookupResultImpl& b) { return !b.ptr; }
        friend bool operator!=(std::nullptr_t, const LookupResultImpl& b) {
            return b.ptr != nullptr;
        }
    };

    using LookupResult = LookupResultImpl;
    using ConstLookupResult = LookupResultImpl;

    using iterator_impl = typename HashMapImpl::iterator;
    using const_iterator_impl = typename HashMapImpl::const_iterator;

    StringSubMap() = default;
    explicit StringSubMap(size_t reserve_for_num_elements) {
        _hash_map.reserve(reserve_for_num_elements);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        inserted = false;
        auto iter = _hash_map.lazy_emplace_with_hash(key, hash_value, [&](const auto& ctor) {
            inserted = true;
            if constexpr (std::is_pointer_v<std::remove_reference_t<Mapped>>) {
                ctor(key, nullptr);
            } else {
                ctor(key, Mapped());
            }
        });
        it = LookupResult(&*iter);
    }

    template <typename KeyHolder, typename Origin, typename Func>
    void ALWAYS_INLINE lazy_emplace_with_origin(KeyHolder&& key, Origin&& origin, LookupResult& it,
                                                size_t hash_value, Func&& f) {
        auto iter = _hash_map.lazy_emplace_with_hash(key, hash_value, [&](const auto& ctor) {
            // The ctor from phmap expects (key, mapped). We need to adapt the
            // callback signature: f(Constructor, key, origin)
            std::forward<Func>(f)(ctor, key, origin);
        });
        it = LookupResult(&*iter);
    }

    LookupResult ALWAYS_INLINE find(const Key& key, size_t hash_value) {
        auto iter = _hash_map.find(key, hash_value);
        if (iter != _hash_map.end()) {
            return LookupResult(&*iter);
        }
        return LookupResult(nullptr);
    }

    ConstLookupResult ALWAYS_INLINE find(const Key& key, size_t hash_value) const {
        auto iter = _hash_map.find(key, hash_value);
        if (iter != _hash_map.end()) {
            return ConstLookupResult(const_cast<typename LookupResult::pair_type*>(&*iter));
        }
        return ConstLookupResult(nullptr);
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch(size_t hash_value) {
        _hash_map.prefetch_hash(hash_value);
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch(const Key& key, size_t hash_value) {
        prefetch<READ>(hash_value);
    }

    size_t size() const { return _hash_map.size(); }
    bool empty() const { return _hash_map.empty(); }

    size_t get_buffer_size_in_bytes() const {
        const auto capacity = _hash_map.capacity();
        return capacity * sizeof(typename HashMapImpl::slot_type);
    }

    bool add_elem_size_overflow(size_t row) const {
        const auto capacity = _hash_map.capacity();
        // phmap use 7/8th as maximum load factor.
        return (_hash_map.size() + row) > (capacity * 7 / 8);
    }

    size_t estimate_memory(size_t num_elem) const {
        if (!add_elem_size_overflow(num_elem)) {
            return 0;
        }
        auto new_size = _hash_map.capacity() * 2 + 1;
        return phmap::priv::hashtable_debug_internal::HashtableDebugAccess<
                HashMapImpl>::LowerBoundAllocatedByteSize(new_size);
    }

    /// Call func(Mapped &) for each element.
    template <typename Func>
    void for_each_mapped(Func&& func) {
        for (auto& v : _hash_map) {
            func(v.second);
        }
    }

    size_t hash(const Key& x) const { return Hash()(x); }

    // Iterator wrapper that provides get_first/get_second/get_mapped interface
    // compatible with the StringHashTable outer iterator.
    template <typename Derived, bool is_const>
    class iterator_base {
        using BaseIterator = std::conditional_t<is_const, const_iterator_impl, iterator_impl>;
        BaseIterator base_iterator;
        friend class StringSubMap;

    public:
        iterator_base() = default;
        iterator_base(BaseIterator it) : base_iterator(it) {}

        bool operator==(const iterator_base& rhs) const {
            return base_iterator == rhs.base_iterator;
        }
        bool operator!=(const iterator_base& rhs) const {
            return base_iterator != rhs.base_iterator;
        }

        Derived& operator++() {
            ++base_iterator;
            return static_cast<Derived&>(*this);
        }

        auto& operator*() { return *this; }
        auto& operator*() const { return *this; }
        auto* operator->() { return this; }
        auto* operator->() const { return this; }

        const auto& get_first() const { return base_iterator->first; }
        const auto& get_second() const { return base_iterator->second; }
        auto& get_second() { return const_cast<Mapped&>(base_iterator->second); }

        auto& get_mapped() { return const_cast<Mapped&>(base_iterator->second); }
        const auto& get_mapped() const { return base_iterator->second; }

        // For the outer iterator: get_key() as StringRef
        doris::StringRef get_key() const {
            if constexpr (std::is_same_v<Key, doris::StringRef>) {
                return base_iterator->first;
            } else {
                return to_string_ref(base_iterator->first);
            }
        }

        auto get_ptr() const { return this; }
    };

    class iterator : public iterator_base<iterator, false> {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };

    const_iterator begin() const { return const_iterator(_hash_map.cbegin()); }
    const_iterator cbegin() const { return const_iterator(_hash_map.cbegin()); }
    iterator begin() { return iterator(_hash_map.begin()); }

    const_iterator end() const { return const_iterator(_hash_map.cend()); }
    const_iterator cend() const { return const_iterator(_hash_map.cend()); }
    iterator end() { return iterator(_hash_map.end()); }

    void clear_and_shrink() { _hash_map.clear(); }
    void reserve(size_t num_elem) { _hash_map.reserve(num_elem); }

private:
    HashMapImpl _hash_map;
};

template <typename TMapped>
struct StringHashMapSubMaps {
    using T0 = StringHashTableEmpty<StringHashMapCell<doris::StringRef, TMapped>>;
    using T1 = StringSubMap<StringHashMapSubKeys::T1, TMapped, StringHashTableHash>;
    using T2 = StringSubMap<StringHashMapSubKeys::T2, TMapped, StringHashTableHash>;
    using T3 = StringSubMap<StringHashMapSubKeys::T3, TMapped, StringHashTableHash>;
    using T4 = StringSubMap<StringHashMapSubKeys::T4, TMapped, StringHashTableHash>;
    using Ts = StringSubMap<doris::StringRef, TMapped, StringHashTableHash>;
};

template <typename TMapped, typename Allocator = Allocator<true, true>>
class StringHashMap : public StringHashTable<StringHashMapSubMaps<TMapped>> {
public:
    using Key = doris::StringRef;
    using Base = StringHashTable<StringHashMapSubMaps<TMapped>>;
    using Self = StringHashMap;
    using LookupResult = typename Base::LookupResult;
    using Value = TMapped;

    using Base::Base;

    TMapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted, this->hash(x));
        if (inserted) {
            new (&it->get_mapped()) TMapped();
        }

        return it->get_mapped();
    }

    template <typename Func>
    void ALWAYS_INLINE for_each_mapped(Func&& func) {
        if (this->m0.size()) {
            func(this->m0.zero_value()->get_second());
        }
        for (auto& v : this->m1) {
            func(v.get_second());
        }
        for (auto& v : this->m2) {
            func(v.get_second());
        }
        for (auto& v : this->m3) {
            func(v.get_second());
        }
        for (auto& v : this->m4) {
            func(v.get_second());
        }
        for (auto& v : this->ms) {
            func(v.get_second());
        }
    }
    template <typename MappedType>
    char* get_null_key_data() {
        return nullptr;
    }
    bool has_null_key_data() const { return false; }
};
} // namespace doris
