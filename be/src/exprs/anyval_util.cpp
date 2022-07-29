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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/anyval-util.cc
// and modified by Doris

#include "exprs/anyval_util.h"

#include "common/object_pool.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/mem_tracker.h"

namespace doris {
using doris_udf::BooleanVal;
using doris_udf::TinyIntVal;
using doris_udf::SmallIntVal;
using doris_udf::IntVal;
using doris_udf::BigIntVal;
using doris_udf::LargeIntVal;
using doris_udf::FloatVal;
using doris_udf::DoubleVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::StringVal;
using doris_udf::AnyVal;
using doris_udf::DateV2Val;
using doris_udf::DateTimeV2Val;

Status allocate_any_val(RuntimeState* state, MemPool* pool, const TypeDescriptor& type,
                        const std::string& mem_limit_exceeded_msg, AnyVal** result) {
    const int anyval_size = AnyValUtil::any_val_size(type);
    const int anyval_alignment = AnyValUtil::any_val_alignment(type);
    Status rst;
    *result = reinterpret_cast<AnyVal*>(
            pool->try_allocate_aligned(anyval_size, anyval_alignment, &rst));
    if (*result == nullptr) {
        RETURN_LIMIT_EXCEEDED(state, mem_limit_exceeded_msg, anyval_size, rst);
    }
    memset(static_cast<void*>(*result), 0, anyval_size);
    return Status::OK();
}

AnyVal* create_any_val(ObjectPool* pool, const TypeDescriptor& type) {
    switch (type.type) {
    case PrimitiveType::TYPE_NULL:
        return pool->add(new AnyVal);

    case PrimitiveType::TYPE_BOOLEAN:
        return pool->add(new BooleanVal);

    case PrimitiveType::TYPE_TINYINT:
        return pool->add(new TinyIntVal);

    case PrimitiveType::TYPE_SMALLINT:
        return pool->add(new SmallIntVal);

    case PrimitiveType::TYPE_INT:
        return pool->add(new IntVal);

    case PrimitiveType::TYPE_BIGINT:
        return pool->add(new BigIntVal);

    case PrimitiveType::TYPE_LARGEINT:
        return pool->add(new LargeIntVal);

    case PrimitiveType::TYPE_FLOAT:
        return pool->add(new FloatVal);

    case PrimitiveType::TYPE_TIME:
    case PrimitiveType::TYPE_TIMEV2:
    case PrimitiveType::TYPE_DOUBLE:
        return pool->add(new DoubleVal);

    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_HLL:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_OBJECT:
    case PrimitiveType::TYPE_QUANTILE_STATE:
    case PrimitiveType::TYPE_STRING:
        return pool->add(new StringVal);

    case PrimitiveType::TYPE_DECIMALV2:
        return pool->add(new DecimalV2Val);

    case PrimitiveType::TYPE_DECIMAL32:
        return pool->add(new IntVal);

    case PrimitiveType::TYPE_DECIMAL64:
        return pool->add(new BigIntVal);

    case PrimitiveType::TYPE_DECIMAL128:
        return pool->add(new LargeIntVal);

    case PrimitiveType::TYPE_DATE:
        return pool->add(new DateTimeVal);

    case PrimitiveType::TYPE_DATEV2:
        return pool->add(new DateV2Val);

    case PrimitiveType::TYPE_DATETIMEV2:
        return pool->add(new DateTimeV2Val);

    case PrimitiveType::TYPE_DATETIME:
        return pool->add(new DateTimeVal);

    case PrimitiveType::TYPE_ARRAY:
        return pool->add(new CollectionVal);

    default:
        DCHECK(false) << "Unsupported type: " << type.type;
        return nullptr;
    }
}

FunctionContext::TypeDesc AnyValUtil::column_type_to_type_desc(const TypeDescriptor& type) {
    FunctionContext::TypeDesc out;
    switch (type.type) {
    case PrimitiveType::TYPE_BOOLEAN:
        out.type = FunctionContext::TYPE_BOOLEAN;
        break;
    case PrimitiveType::TYPE_TINYINT:
        out.type = FunctionContext::TYPE_TINYINT;
        break;
    case PrimitiveType::TYPE_SMALLINT:
        out.type = FunctionContext::TYPE_SMALLINT;
        break;
    case PrimitiveType::TYPE_INT:
        out.type = FunctionContext::TYPE_INT;
        break;
    case PrimitiveType::TYPE_BIGINT:
        out.type = FunctionContext::TYPE_BIGINT;
        break;
    case PrimitiveType::TYPE_LARGEINT:
        out.type = FunctionContext::TYPE_LARGEINT;
        break;
    case PrimitiveType::TYPE_FLOAT:
        out.type = FunctionContext::TYPE_FLOAT;
        break;
    case PrimitiveType::TYPE_TIME:
    case PrimitiveType::TYPE_TIMEV2:
    case PrimitiveType::TYPE_DOUBLE:
        out.type = FunctionContext::TYPE_DOUBLE;
        break;
    case PrimitiveType::TYPE_DATE:
        out.type = FunctionContext::TYPE_DATE;
        break;
    case PrimitiveType::TYPE_DATETIME:
        out.type = FunctionContext::TYPE_DATETIME;
        break;
    case PrimitiveType::TYPE_DATEV2:
        out.type = FunctionContext::TYPE_DATEV2;
        break;
    case PrimitiveType::TYPE_DATETIMEV2:
        out.type = FunctionContext::TYPE_DATETIMEV2;
        break;
    case PrimitiveType::TYPE_DECIMAL32:
        out.type = FunctionContext::TYPE_DECIMAL32;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case PrimitiveType::TYPE_DECIMAL64:
        out.type = FunctionContext::TYPE_DECIMAL64;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case PrimitiveType::TYPE_DECIMAL128:
        out.type = FunctionContext::TYPE_DECIMAL128;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case PrimitiveType::TYPE_VARCHAR:
        out.type = FunctionContext::TYPE_VARCHAR;
        out.len = type.len;
        break;
    case PrimitiveType::TYPE_HLL:
        out.type = FunctionContext::TYPE_HLL;
        out.len = type.len;
        break;
    case PrimitiveType::TYPE_OBJECT:
        out.type = FunctionContext::TYPE_OBJECT;
        // FIXME(cmy): is this fallthrough meaningful?
    case PrimitiveType::TYPE_QUANTILE_STATE:
        out.type = FunctionContext::TYPE_QUANTILE_STATE;
        break;
    case PrimitiveType::TYPE_CHAR:
        out.type = FunctionContext::TYPE_CHAR;
        out.len = type.len;
        break;
    case PrimitiveType::TYPE_DECIMALV2:
        out.type = FunctionContext::TYPE_DECIMALV2;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case PrimitiveType::TYPE_NULL:
        out.type = FunctionContext::TYPE_NULL;
        break;
    case PrimitiveType::TYPE_ARRAY:
        out.type = FunctionContext::TYPE_ARRAY;
        for (const auto& t : type.children) {
            out.children.push_back(column_type_to_type_desc(t));
        }
        break;
    case PrimitiveType::TYPE_STRING:
        out.type = FunctionContext::TYPE_STRING;
        out.len = type.len;
        break;
    default:
        DCHECK(false) << "Unknown type: " << type;
    }
    return out;
}

} // namespace doris
