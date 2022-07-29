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

#include "exec/schema_scanner/schema_statistics_scanner.h"

#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaStatisticsScanner::_s_cols_statistics[] = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"NON_UNIQUE", PrimitiveType::TYPE_BIGINT, sizeof(int64_t), false},
        {"INDEX_SCHEMA", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"INDEX_NAME", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"SEQ_IN_INDEX", PrimitiveType::TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_NAME", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLLATION", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), true},
        {"CARDINALITY", PrimitiveType::TYPE_BIGINT, sizeof(int64_t), true},
        {"SUB_PART", PrimitiveType::TYPE_BIGINT, sizeof(int64_t), true},
        {"PACKED", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), true},
        {"NULLABLE", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"INDEX_TYPE", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), false},
        {"COMMENT", PrimitiveType::TYPE_VARCHAR, sizeof(StringValue), true},
};

SchemaStatisticsScanner::SchemaStatisticsScanner()
        : SchemaScanner(_s_cols_statistics,
                        sizeof(_s_cols_statistics) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaStatisticsScanner::~SchemaStatisticsScanner() {}

} // namespace doris
