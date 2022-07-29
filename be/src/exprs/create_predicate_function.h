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

#include "exprs/bloomfilter_predicate.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new (std::nothrow) MinMaxNumFunc<typename PrimitiveTypeTraits<type>::CppType>();
    };
};

template <bool is_vec>
class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringValueSet,
                                       HybridSet<type, is_vec>>;
        return new (std::nothrow) Set();
    };
};

class BloomFilterTraits {
public:
    using BasePtr = IBloomFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new BloomFilterFunc<type, CurrentBloomFilterAdaptor>();
    };
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type>
    static typename Traits::BasePtr create() {
        return Traits::template get_function<type>();
    }
};

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case PrimitiveType::TYPE_BOOLEAN:
        return Creator::template create<PrimitiveType::TYPE_BOOLEAN>();
    case PrimitiveType::TYPE_TINYINT:
        return Creator::template create<PrimitiveType::TYPE_TINYINT>();
    case PrimitiveType::TYPE_SMALLINT:
        return Creator::template create<PrimitiveType::TYPE_SMALLINT>();
    case PrimitiveType::TYPE_INT:
        return Creator::template create<PrimitiveType::TYPE_INT>();
    case PrimitiveType::TYPE_BIGINT:
        return Creator::template create<PrimitiveType::TYPE_BIGINT>();
    case PrimitiveType::TYPE_LARGEINT:
        return Creator::template create<PrimitiveType::TYPE_LARGEINT>();

    case PrimitiveType::TYPE_FLOAT:
        return Creator::template create<PrimitiveType::TYPE_FLOAT>();
    case PrimitiveType::TYPE_DOUBLE:
        return Creator::template create<PrimitiveType::TYPE_DOUBLE>();

    case PrimitiveType::TYPE_DECIMALV2:
        return Creator::template create<PrimitiveType::TYPE_DECIMALV2>();

    case PrimitiveType::TYPE_DATE:
        return Creator::template create<PrimitiveType::TYPE_DATE>();
    case PrimitiveType::TYPE_DATETIME:
        return Creator::template create<PrimitiveType::TYPE_DATETIME>();
    case PrimitiveType::TYPE_DATEV2:
        return Creator::template create<PrimitiveType::TYPE_DATEV2>();
    case PrimitiveType::TYPE_DATETIMEV2:
        return Creator::template create<PrimitiveType::TYPE_DATETIMEV2>();

    case PrimitiveType::TYPE_CHAR:
        return Creator::template create<PrimitiveType::TYPE_CHAR>();
    case PrimitiveType::TYPE_VARCHAR:
        return Creator::template create<PrimitiveType::TYPE_VARCHAR>();
    case PrimitiveType::TYPE_STRING:
        return Creator::template create<PrimitiveType::TYPE_STRING>();
    case PrimitiveType::TYPE_DECIMAL32:
        return Creator::template create<PrimitiveType::TYPE_DECIMAL32>();
    case PrimitiveType::TYPE_DECIMAL64:
        return Creator::template create<PrimitiveType::TYPE_DECIMAL64>();
    case PrimitiveType::TYPE_DECIMAL128:
        return Creator::template create<PrimitiveType::TYPE_DECIMAL128>();

    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type) {
    return create_predicate_function<MinmaxFunctionTraits>(type);
}

inline auto create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits<false>>(type);
}

// used for VInPredicate
inline auto vec_create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits<true>>(type);
}

inline auto create_bloom_filter(PrimitiveType type) {
    return create_predicate_function<BloomFilterTraits>(type);
}

} // namespace doris
