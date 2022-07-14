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

#include <charconv>

#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_cond.h"
#include "olap/tablet_schema.h"
#include "util/date_func.h"

namespace doris {

template <typename ConditionType>
class PredicateCreator {
public:
    virtual ColumnPredicate* create(const TabletColumn& column, int index,
                                    const ConditionType& conditions, bool opposite,
                                    MemPool* pool) = 0;
    virtual ~PredicateCreator() = default;
};

template <typename CppType, PredicateType PT, typename ConditionType>
class IntergerPredicateCreator : public PredicateCreator<ConditionType> {
public:
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            phmap::flat_hash_set<CppType> values;
            for (auto condition : conditions) {
                values.insert(convert(condition));
            }
            return new InListPredicateBase<CppType, PT>(index, std::move(values), opposite);
        } else if constexpr (PredicateTypeTraits::is_comparison(PT)) {
            return new ComparisonPredicateBase<CppType, PT>(index, convert(conditions), opposite);
        }
        return nullptr;
    }

private:
    CppType convert(const std::string& condition) {
        CppType value = 0;
        std::from_chars(condition.data(), condition.data() + condition.size(), value);
        return value;
    }
};

template <typename CppType, PredicateType PT, typename ConditionType>
class DecimalPredicateCreator : public PredicateCreator<ConditionType> {
public:
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            phmap::flat_hash_set<CppType> values;
            for (auto condition : conditions) {
                values.insert(convert(column, condition));
            }
            return new InListPredicateBase<CppType, PT>(index, std::move(values), opposite);
        } else if constexpr (PredicateTypeTraits::is_comparison(PT)) {
            return new ComparisonPredicateBase<CppType, PT>(index, convert(column, conditions),
                                                            opposite);
        }
        return nullptr;
    }

private:
    CppType convert(const TabletColumn& column, const std::string& condition) {
        StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;
        // return CppType value cast from int128_t
        return StringParser::string_to_decimal<int128_t>(
                condition.data(), condition.size(), column.precision(), column.frac(), &result);
    }
};

template <PredicateType PT, typename ConditionType>
class StringPredicateCreator : public PredicateCreator<ConditionType> {
public:
    StringPredicateCreator(bool should_padding) : _should_padding(should_padding) {};

    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            phmap::flat_hash_set<StringValue> values;
            for (auto condition : conditions) {
                values.insert(convert(column, condition, pool));
            }
            return new InListPredicateBase<StringValue, PT>(index, std::move(values), opposite);
        } else if constexpr (PredicateTypeTraits::is_comparison(PT)) {
            return new ComparisonPredicateBase<StringValue, PT>(
                    index, convert(column, conditions, pool), opposite);
        }
        return nullptr;
    }

private:
    bool _should_padding;
    StringValue convert(const TabletColumn& column, const std::string& condition, MemPool* pool) {
        size_t length = condition.length();
        if (_should_padding) {
            length = std::max(static_cast<size_t>(column.length()), length);
        }

        char* buffer = reinterpret_cast<char*>(pool->allocate(length));
        memset(buffer, 0, length);
        memory_copy(buffer, condition.data(), condition.length());

        return StringValue(buffer, length);
    }
};

template <typename CppType, PredicateType PT, typename ConditionType>
struct CustomPredicateCreator : public PredicateCreator<ConditionType> {
public:
    CustomPredicateCreator(std::function<CppType(const std::string& condition)> convert)
            : _convert(convert) {};

    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            phmap::flat_hash_set<CppType> values;
            for (auto condition : conditions) {
                values.insert(_convert(condition));
            }
            return new InListPredicateBase<CppType, PT>(index, std::move(values), opposite);
        } else if constexpr (PredicateTypeTraits::is_comparison(PT)) {
            return new ComparisonPredicateBase<CppType, PT>(index, _convert(conditions), opposite);
        }
        return nullptr;
    }

private:
    std::function<CppType(const std::string& condition)> _convert;
};

template <PredicateType PT, typename ConditionType>
inline std::unique_ptr<PredicateCreator<ConditionType>> get_creator(const FieldType& type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT: {
        return std::make_unique<IntergerPredicateCreator<int8_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        return std::make_unique<IntergerPredicateCreator<int16_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_INT: {
        return std::make_unique<IntergerPredicateCreator<int32_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        return std::make_unique<IntergerPredicateCreator<int64_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        return std::make_unique<IntergerPredicateCreator<int128_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        return std::make_unique<CustomPredicateCreator<decimal12_t, PT, ConditionType>>(
                [](const std::string& condition) {
                    decimal12_t value = {0, 0};
                    value.from_string(condition);
                    return value;
                });
    }
    case OLAP_FIELD_TYPE_DECIMAL32: {
        return std::make_unique<DecimalPredicateCreator<int32_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL64: {
        return std::make_unique<DecimalPredicateCreator<int64_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL128: {
        return std::make_unique<DecimalPredicateCreator<int128_t, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<StringPredicateCreator<PT, ConditionType>>(true);
    }
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING: {
        return std::make_unique<StringPredicateCreator<PT, ConditionType>>(false);
    }
    case OLAP_FIELD_TYPE_DATE: {
        return std::make_unique<CustomPredicateCreator<uint24_t, PT, ConditionType>>(
                timestamp_from_date);
    }
    case OLAP_FIELD_TYPE_DATEV2: {
        return std::make_unique<CustomPredicateCreator<uint32_t, PT, ConditionType>>(
                timestamp_from_date_v2);
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        return std::make_unique<CustomPredicateCreator<uint64_t, PT, ConditionType>>(
                timestamp_from_datetime);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        return std::make_unique<CustomPredicateCreator<bool, PT, ConditionType>>(
                [](const std::string& condition) {
                    int32_t ivalue = 0;
                    auto result = std::from_chars(condition.data(),
                                                  condition.data() + condition.size(), ivalue);
                    if (result.ec == std::errc()) {
                        return bool(ivalue);
                    }

                    StringParser::ParseResult parse_result;
                    bool value = StringParser::string_to_bool(condition.data(), condition.size(),
                                                              &parse_result);
                    return value;
                });
    }
    default:
        return nullptr;
    }
}

template <PredicateType PT, typename ConditionType>
inline ColumnPredicate* create_predicate(const TabletColumn& column, int index,
                                         const ConditionType& conditions, bool opposite,
                                         MemPool* pool) {
    return get_creator<PT, ConditionType>(column.type())
            ->create(column, index, conditions, opposite, pool);
}

template <PredicateType PT>
inline ColumnPredicate* create_comparison_predicate(const TabletColumn& column, int index,
                                                    const std::string& condition, bool opposite,
                                                    MemPool* pool) {
    if (!PredicateTypeTraits::is_comparison(PT)) {
        LOG(FATAL) << "input PredicateType is invalid, PredicateType=" << int(PT);
    }
    return create_predicate<PT, std::string>(column, index, condition, opposite, pool);
}

template <PredicateType PT>
inline ColumnPredicate* create_list_predicate(const TabletColumn& column, int index,
                                              const std::vector<std::string>& conditions,
                                              bool opposite, MemPool* pool) {
    if (column.type() == OLAP_FIELD_TYPE_BOOL) {
        LOG(FATAL) << "Failed to create list preacate! input column type is invalid";
        return nullptr;
    }
    if (!PredicateTypeTraits::is_list(PT)) {
        LOG(FATAL) << "input PredicateType is invalid, PredicateType=" << int(PT);
    }
    return create_predicate<PT, std::vector<std::string>>(column, index, conditions, opposite,
                                                          pool);
}

} //namespace doris
