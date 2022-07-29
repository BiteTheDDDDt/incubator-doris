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

#include "runtime/primitive_type.h"

#include <sstream>

#include "gen_cpp/Types_types.h"
#include "runtime/collection_value.h"
#include "runtime/string_value.h"

namespace doris {

PrimitiveType convert_type_to_primitive(FunctionContext::Type type) {
    switch (type) {
    case FunctionContext::Type::INVALID_TYPE:
        return PrimitiveType::INVALID_TYPE;
    case FunctionContext::Type::TYPE_DOUBLE:
        return PrimitiveType::TYPE_DOUBLE;
    case FunctionContext::Type::TYPE_NULL:
        return PrimitiveType::TYPE_NULL;
    case FunctionContext::Type::TYPE_CHAR:
        return PrimitiveType::TYPE_CHAR;
    case FunctionContext::Type::TYPE_VARCHAR:
        return PrimitiveType::TYPE_VARCHAR;
    case FunctionContext::Type::TYPE_STRING:
        return PrimitiveType::TYPE_STRING;
    case FunctionContext::Type::TYPE_DATETIME:
        return PrimitiveType::TYPE_DATETIME;
    case FunctionContext::Type::TYPE_DECIMALV2:
        return PrimitiveType::TYPE_DECIMALV2;
    case FunctionContext::Type::TYPE_DECIMAL32:
        return PrimitiveType::TYPE_DECIMAL32;
    case FunctionContext::Type::TYPE_DECIMAL64:
        return PrimitiveType::TYPE_DECIMAL64;
    case FunctionContext::Type::TYPE_DECIMAL128:
        return PrimitiveType::TYPE_DECIMAL128;
    case FunctionContext::Type::TYPE_BOOLEAN:
        return PrimitiveType::TYPE_BOOLEAN;
    case FunctionContext::Type::TYPE_ARRAY:
        return PrimitiveType::TYPE_ARRAY;
    case FunctionContext::Type::TYPE_OBJECT:
        return PrimitiveType::TYPE_OBJECT;
    case FunctionContext::Type::TYPE_HLL:
        return PrimitiveType::TYPE_HLL;
    case FunctionContext::Type::TYPE_QUANTILE_STATE:
        return PrimitiveType::TYPE_QUANTILE_STATE;
    case FunctionContext::Type::TYPE_TINYINT:
        return PrimitiveType::TYPE_TINYINT;
    case FunctionContext::Type::TYPE_SMALLINT:
        return PrimitiveType::TYPE_SMALLINT;
    case FunctionContext::Type::TYPE_INT:
        return PrimitiveType::TYPE_INT;
    case FunctionContext::Type::TYPE_BIGINT:
        return PrimitiveType::TYPE_BIGINT;
    case FunctionContext::Type::TYPE_LARGEINT:
        return PrimitiveType::TYPE_LARGEINT;
    case FunctionContext::Type::TYPE_DATE:
        return PrimitiveType::TYPE_DATE;
    case FunctionContext::Type::TYPE_DATEV2:
        return PrimitiveType::TYPE_DATEV2;
    case FunctionContext::Type::TYPE_DATETIMEV2:
        return PrimitiveType::TYPE_DATETIMEV2;
    case FunctionContext::Type::TYPE_TIMEV2:
        return PrimitiveType::TYPE_TIMEV2;
    default:
        DCHECK(false);
    }

    return PrimitiveType::INVALID_TYPE;
}

bool is_enumeration_type(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DOUBLE:
    case PrimitiveType::TYPE_NULL:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATETIMEV2:
    case PrimitiveType::TYPE_TIMEV2:
    case PrimitiveType::TYPE_DECIMALV2:
    case PrimitiveType::TYPE_DECIMAL32:
    case PrimitiveType::TYPE_DECIMAL64:
    case PrimitiveType::TYPE_DECIMAL128:
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_ARRAY:
    case PrimitiveType::TYPE_HLL:
        return false;
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_LARGEINT:
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATEV2:
        return true;

    case PrimitiveType::INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return false;
}

bool is_date_type(PrimitiveType type) {
    return type == PrimitiveType::TYPE_DATETIME || type == PrimitiveType::TYPE_DATE ||
           type == PrimitiveType::TYPE_DATETIMEV2 || type == PrimitiveType::TYPE_DATEV2;
}

bool is_string_type(PrimitiveType type) {
    return type == PrimitiveType::TYPE_CHAR || type == PrimitiveType::TYPE_VARCHAR ||
           type == PrimitiveType::TYPE_STRING;
}

bool has_variable_type(PrimitiveType type) {
    return type == PrimitiveType::TYPE_CHAR || type == PrimitiveType::TYPE_VARCHAR ||
           type == PrimitiveType::TYPE_OBJECT || type == PrimitiveType::TYPE_QUANTILE_STATE ||
           type == PrimitiveType::TYPE_STRING;
}

// Returns the byte size of 'type'  Returns 0 for variable length types.
int get_byte_size(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_OBJECT:
    case PrimitiveType::TYPE_HLL:
    case PrimitiveType::TYPE_QUANTILE_STATE:
    case PrimitiveType::TYPE_ARRAY:
    case PrimitiveType::TYPE_MAP:
        return 0;

    case PrimitiveType::TYPE_NULL:
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_TINYINT:
        return 1;

    case PrimitiveType::TYPE_SMALLINT:
        return 2;

    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DECIMAL32:
        return 4;

    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_DOUBLE:
    case PrimitiveType::TYPE_TIME:
    case PrimitiveType::TYPE_DECIMAL64:
        return 8;

    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_LARGEINT:
    case PrimitiveType::TYPE_DECIMALV2:
    case PrimitiveType::TYPE_DECIMAL128:
        return 16;

    case PrimitiveType::INVALID_TYPE:
    // datev2/datetimev2/timev2 is not supported on row-based engine
    case PrimitiveType::TYPE_DATEV2:
    case PrimitiveType::TYPE_DATETIMEV2:
    case PrimitiveType::TYPE_TIMEV2:
    default:
        DCHECK(false);
    }

    return 0;
}

bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    if (lhs == PrimitiveType::TYPE_VARCHAR) {
        return rhs == PrimitiveType::TYPE_CHAR || rhs == PrimitiveType::TYPE_VARCHAR ||
               rhs == PrimitiveType::TYPE_HLL || rhs == PrimitiveType::TYPE_OBJECT ||
               rhs == PrimitiveType::TYPE_QUANTILE_STATE || rhs == PrimitiveType::TYPE_STRING;
    }

    if (lhs == PrimitiveType::TYPE_OBJECT) {
        return rhs == PrimitiveType::TYPE_VARCHAR || rhs == PrimitiveType::TYPE_OBJECT ||
               rhs == PrimitiveType::TYPE_STRING;
    }

    if (lhs == PrimitiveType::TYPE_CHAR || lhs == PrimitiveType::TYPE_HLL) {
        return rhs == PrimitiveType::TYPE_CHAR || rhs == PrimitiveType::TYPE_VARCHAR ||
               rhs == PrimitiveType::TYPE_HLL || rhs == PrimitiveType::TYPE_STRING;
    }

    if (lhs == PrimitiveType::TYPE_STRING) {
        return rhs == PrimitiveType::TYPE_CHAR || rhs == PrimitiveType::TYPE_VARCHAR ||
               rhs == PrimitiveType::TYPE_HLL || rhs == PrimitiveType::TYPE_OBJECT ||
               rhs == PrimitiveType::TYPE_STRING;
    }

    if (lhs == PrimitiveType::TYPE_QUANTILE_STATE) {
        return rhs == PrimitiveType::TYPE_VARCHAR || rhs == PrimitiveType::TYPE_QUANTILE_STATE ||
               rhs == PrimitiveType::TYPE_STRING;
    }

    return lhs == rhs;
}

//to_tcolumn_type_thrift only test
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype) {
    TColumnType t;
    t.__set_type(ttype);
    return t;
}

TExprOpcode::type to_in_opcode(PrimitiveType t) {
    return TExprOpcode::FILTER_IN;
}

PrimitiveType thrift_to_type(TPrimitiveType::type ttype) {
    switch (ttype) {
    case TPrimitiveType::INVALID_TYPE:
        return PrimitiveType::INVALID_TYPE;

    case TPrimitiveType::NULL_TYPE:
        return PrimitiveType::TYPE_NULL;

    case TPrimitiveType::BOOLEAN:
        return PrimitiveType::TYPE_BOOLEAN;

    case TPrimitiveType::TINYINT:
        return PrimitiveType::TYPE_TINYINT;

    case TPrimitiveType::SMALLINT:
        return PrimitiveType::TYPE_SMALLINT;

    case TPrimitiveType::INT:
        return PrimitiveType::TYPE_INT;

    case TPrimitiveType::BIGINT:
        return PrimitiveType::TYPE_BIGINT;

    case TPrimitiveType::LARGEINT:
        return PrimitiveType::TYPE_LARGEINT;

    case TPrimitiveType::FLOAT:
        return PrimitiveType::TYPE_FLOAT;

    case TPrimitiveType::DOUBLE:
        return PrimitiveType::TYPE_DOUBLE;

    case TPrimitiveType::DATE:
        return PrimitiveType::TYPE_DATE;

    case TPrimitiveType::DATETIME:
        return PrimitiveType::TYPE_DATETIME;

    case TPrimitiveType::DATEV2:
        return PrimitiveType::TYPE_DATEV2;

    case TPrimitiveType::DATETIMEV2:
        return PrimitiveType::TYPE_DATETIMEV2;

    case TPrimitiveType::TIMEV2:
        return PrimitiveType::TYPE_TIMEV2;

    case TPrimitiveType::TIME:
        return PrimitiveType::TYPE_TIME;

    case TPrimitiveType::VARCHAR:
        return PrimitiveType::TYPE_VARCHAR;

    case TPrimitiveType::STRING:
        return PrimitiveType::TYPE_STRING;

    case TPrimitiveType::BINARY:
        return PrimitiveType::TYPE_BINARY;

    case TPrimitiveType::DECIMALV2:
        return PrimitiveType::TYPE_DECIMALV2;

    case TPrimitiveType::DECIMAL32:
        return PrimitiveType::TYPE_DECIMAL32;

    case TPrimitiveType::DECIMAL64:
        return PrimitiveType::TYPE_DECIMAL64;

    case TPrimitiveType::DECIMAL128:
        return PrimitiveType::TYPE_DECIMAL128;

    case TPrimitiveType::CHAR:
        return PrimitiveType::TYPE_CHAR;

    case TPrimitiveType::HLL:
        return PrimitiveType::TYPE_HLL;

    case TPrimitiveType::OBJECT:
        return PrimitiveType::TYPE_OBJECT;

    case TPrimitiveType::QUANTILE_STATE:
        return PrimitiveType::TYPE_QUANTILE_STATE;

    case TPrimitiveType::ARRAY:
        return PrimitiveType::TYPE_ARRAY;

    default:
        return PrimitiveType::INVALID_TYPE;
    }
}

TPrimitiveType::type to_thrift(PrimitiveType ptype) {
    switch (ptype) {
    case PrimitiveType::INVALID_TYPE:
        return TPrimitiveType::INVALID_TYPE;

    case PrimitiveType::TYPE_NULL:
        return TPrimitiveType::NULL_TYPE;

    case PrimitiveType::TYPE_BOOLEAN:
        return TPrimitiveType::BOOLEAN;

    case PrimitiveType::TYPE_TINYINT:
        return TPrimitiveType::TINYINT;

    case PrimitiveType::TYPE_SMALLINT:
        return TPrimitiveType::SMALLINT;

    case PrimitiveType::TYPE_INT:
        return TPrimitiveType::INT;

    case PrimitiveType::TYPE_BIGINT:
        return TPrimitiveType::BIGINT;

    case PrimitiveType::TYPE_LARGEINT:
        return TPrimitiveType::LARGEINT;

    case PrimitiveType::TYPE_FLOAT:
        return TPrimitiveType::FLOAT;

    case PrimitiveType::TYPE_DOUBLE:
        return TPrimitiveType::DOUBLE;

    case PrimitiveType::TYPE_DATE:
        return TPrimitiveType::DATE;

    case PrimitiveType::TYPE_DATETIME:
        return TPrimitiveType::DATETIME;

    case PrimitiveType::TYPE_TIME:
        return TPrimitiveType::TIME;

    case PrimitiveType::TYPE_DATEV2:
        return TPrimitiveType::DATEV2;

    case PrimitiveType::TYPE_DATETIMEV2:
        return TPrimitiveType::DATETIMEV2;

    case PrimitiveType::TYPE_TIMEV2:
        return TPrimitiveType::TIMEV2;

    case PrimitiveType::TYPE_VARCHAR:
        return TPrimitiveType::VARCHAR;

    case PrimitiveType::TYPE_STRING:
        return TPrimitiveType::STRING;

    case PrimitiveType::TYPE_BINARY:
        return TPrimitiveType::BINARY;

    case PrimitiveType::TYPE_DECIMALV2:
        return TPrimitiveType::DECIMALV2;

    case PrimitiveType::TYPE_DECIMAL32:
        return TPrimitiveType::DECIMAL32;

    case PrimitiveType::TYPE_DECIMAL64:
        return TPrimitiveType::DECIMAL64;

    case PrimitiveType::TYPE_DECIMAL128:
        return TPrimitiveType::DECIMAL128;

    case PrimitiveType::TYPE_CHAR:
        return TPrimitiveType::CHAR;

    case PrimitiveType::TYPE_HLL:
        return TPrimitiveType::HLL;

    case PrimitiveType::TYPE_OBJECT:
        return TPrimitiveType::OBJECT;

    case PrimitiveType::TYPE_QUANTILE_STATE:
        return TPrimitiveType::QUANTILE_STATE;

    case PrimitiveType::TYPE_ARRAY:
        return TPrimitiveType::ARRAY;

    default:
        return TPrimitiveType::INVALID_TYPE;
    }
}

std::string type_to_string(PrimitiveType t) {
    switch (t) {
    case PrimitiveType::INVALID_TYPE:
        return "INVALID";

    case PrimitiveType::TYPE_NULL:
        return "NULL";

    case PrimitiveType::TYPE_BOOLEAN:
        return "BOOL";

    case PrimitiveType::TYPE_TINYINT:
        return "TINYINT";

    case PrimitiveType::TYPE_SMALLINT:
        return "SMALLINT";

    case PrimitiveType::TYPE_INT:
        return "INT";

    case PrimitiveType::TYPE_BIGINT:
        return "BIGINT";

    case PrimitiveType::TYPE_LARGEINT:
        return "LARGEINT";

    case PrimitiveType::TYPE_FLOAT:
        return "FLOAT";

    case PrimitiveType::TYPE_DOUBLE:
        return "DOUBLE";

    case PrimitiveType::TYPE_DATE:
        return "DATE";

    case PrimitiveType::TYPE_DATETIME:
        return "DATETIME";

    case PrimitiveType::TYPE_TIME:
        return "TIME";

    case PrimitiveType::TYPE_DATEV2:
        return "DATEV2";

    case PrimitiveType::TYPE_DATETIMEV2:
        return "DATETIMEV2";

    case PrimitiveType::TYPE_TIMEV2:
        return "TIMEV2";

    case PrimitiveType::TYPE_VARCHAR:
        return "VARCHAR";

    case PrimitiveType::TYPE_STRING:
        return "STRING";

    case PrimitiveType::TYPE_BINARY:
        return "BINARY";

    case PrimitiveType::TYPE_DECIMALV2:
        return "DECIMALV2";

    case PrimitiveType::TYPE_DECIMAL32:
        return "DECIMAL32";

    case PrimitiveType::TYPE_DECIMAL64:
        return "DECIMAL64";

    case PrimitiveType::TYPE_DECIMAL128:
        return "DECIMAL128";

    case PrimitiveType::TYPE_CHAR:
        return "CHAR";

    case PrimitiveType::TYPE_HLL:
        return "HLL";

    case PrimitiveType::TYPE_OBJECT:
        return "OBJECT";

    case PrimitiveType::TYPE_QUANTILE_STATE:
        return "QUANTILE_STATE";

    case PrimitiveType::TYPE_ARRAY:
        return "ARRAY";

    default:
        return "";
    };

    return "";
}

std::string type_to_odbc_string(PrimitiveType t) {
    // ODBC driver requires types in lower case
    switch (t) {
    default:
    case PrimitiveType::INVALID_TYPE:
        return "invalid";

    case PrimitiveType::TYPE_NULL:
        return "null";

    case PrimitiveType::TYPE_BOOLEAN:
        return "boolean";

    case PrimitiveType::TYPE_TINYINT:
        return "tinyint";

    case PrimitiveType::TYPE_SMALLINT:
        return "smallint";

    case PrimitiveType::TYPE_INT:
        return "int";

    case PrimitiveType::TYPE_BIGINT:
        return "bigint";

    case PrimitiveType::TYPE_LARGEINT:
        return "largeint";

    case PrimitiveType::TYPE_FLOAT:
        return "float";

    case PrimitiveType::TYPE_DOUBLE:
        return "double";

    case PrimitiveType::TYPE_DATE:
        return "date";

    case PrimitiveType::TYPE_DATETIME:
        return "datetime";

    case PrimitiveType::TYPE_DATEV2:
        return "datev2";

    case PrimitiveType::TYPE_DATETIMEV2:
        return "datetimev2";

    case PrimitiveType::TYPE_TIMEV2:
        return "timev2";

    case PrimitiveType::TYPE_VARCHAR:
        return "string";

    case PrimitiveType::TYPE_STRING:
        return "string";

    case PrimitiveType::TYPE_BINARY:
        return "binary";

    case PrimitiveType::TYPE_DECIMALV2:
        return "decimalv2";

    case PrimitiveType::TYPE_DECIMAL32:
        return "decimal32";

    case PrimitiveType::TYPE_DECIMAL64:
        return "decimal64";

    case PrimitiveType::TYPE_DECIMAL128:
        return "decimal128";

    case PrimitiveType::TYPE_CHAR:
        return "char";

    case PrimitiveType::TYPE_HLL:
        return "hll";

    case PrimitiveType::TYPE_OBJECT:
        return "object";
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return "quantile_state";
    };

    return "unknown";
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

// for test only
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name) {
    std::vector<TTypeNode> types_list;
    TTypeNode type_node;
    TTypeDesc type_desc;
    TScalarType scalar_type;
    scalar_type.__set_type(val);
    std::vector<TStructField> fields;
    TStructField field;
    field.__set_name(name);
    fields.push_back(field);
    type_node.__set_struct_fields(fields);
    type_node.__set_scalar_type(scalar_type);
    types_list.push_back(type_node);
    type_desc.__set_types(types_list);
    return type_desc;
}

int get_slot_size(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_OBJECT:
    case PrimitiveType::TYPE_HLL:
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return sizeof(StringValue);
    case PrimitiveType::TYPE_ARRAY:
        return sizeof(CollectionValue);

    case PrimitiveType::TYPE_NULL:
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_TINYINT:
        return 1;

    case PrimitiveType::TYPE_SMALLINT:
        return 2;

    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_DATEV2:
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DECIMAL32:
        return 4;

    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_DOUBLE:
    case PrimitiveType::TYPE_TIME:
    case PrimitiveType::TYPE_DECIMAL64:
    case PrimitiveType::TYPE_DATETIMEV2:
    case PrimitiveType::TYPE_TIMEV2:
        return 8;

    case PrimitiveType::TYPE_LARGEINT:
        return sizeof(__int128);

    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATETIME:
        // This is the size of the slot, the actual size of the data is 12.
        return sizeof(DateTimeValue);

    case PrimitiveType::TYPE_DECIMALV2:
    case PrimitiveType::TYPE_DECIMAL128:
        return 16;

    case PrimitiveType::INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return 0;
}

} // namespace doris
