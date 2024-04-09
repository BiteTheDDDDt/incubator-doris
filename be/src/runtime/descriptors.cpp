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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/descriptors.cc
// and modified by Doris

#include "runtime/descriptors.h"

#include <fmt/format.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <stddef.h>

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <memory>

#include "common/object_pool.h"
#include "runtime/primitive_type.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

const int RowDescriptor::INVALID_IDX = -1;

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
        : _id(tdesc.id),
          _type(TypeDescriptor::from_thrift(tdesc.slotType)),
          _parent(tdesc.parent),
          _col_pos(tdesc.columnPos),
          _is_nullable(tdesc.nullIndicatorBit != -1),
          _col_name(tdesc.colName),
          _col_name_lower_case(to_lower(tdesc.colName)),
          _col_unique_id(tdesc.col_unique_id),
          _col_type(thrift_to_type(tdesc.primitive_type)),
          _slot_idx(tdesc.slotIdx),
          _field_idx(-1),
          _is_materialized(tdesc.isMaterialized),
          _is_key(tdesc.is_key),
          _need_materialize(tdesc.need_materialize),
          _column_paths(tdesc.column_paths),
          _is_auto_increment(tdesc.__isset.is_auto_increment ? tdesc.is_auto_increment : false),
          _col_default_value(tdesc.__isset.col_default_value ? tdesc.col_default_value : "") {}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
          _type(TypeDescriptor::from_protobuf(pdesc.slot_type())),
          _parent(pdesc.parent()),
          _col_pos(pdesc.column_pos()),
          _is_nullable(pdesc.null_indicator_bit() != -1),
          _col_name(pdesc.col_name()),
          _col_name_lower_case(to_lower(pdesc.col_name())),
          _col_unique_id(pdesc.col_unique_id()),
          _col_type(static_cast<PrimitiveType>(pdesc.col_type())),
          _slot_idx(pdesc.slot_idx()),
          _field_idx(-1),
          _is_materialized(pdesc.is_materialized()),
          _is_key(pdesc.is_key()),
          _need_materialize(true),
          _column_paths(pdesc.column_paths().begin(), pdesc.column_paths().end()),
          _is_auto_increment(pdesc.is_auto_increment()) {}

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    _type.to_protobuf(pslot->mutable_slot_type());
    pslot->set_column_pos(_col_pos);
    pslot->set_byte_offset(0);
    pslot->set_null_indicator_byte(0);
    pslot->set_null_indicator_bit(_is_nullable ? 0 : -1);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_is_materialized(_is_materialized);
    pslot->set_col_unique_id(_col_unique_id);
    pslot->set_is_key(_is_key);
    pslot->set_is_auto_increment(_is_auto_increment);
    pslot->set_col_type(_col_type);
    for (const std::string& path : _column_paths) {
        pslot->add_column_paths(path);
    }
}

vectorized::MutableColumnPtr SlotDescriptor::get_empty_mutable_column() const {
    auto data_type = get_data_type_ptr();
    if (data_type) {
        return data_type->create_column();
    }
    return nullptr;
}

vectorized::DataTypePtr SlotDescriptor::get_data_type_ptr() const {
    return vectorized::DataTypeFactory::instance().create_data_type(type(), is_nullable());
}

std::string SlotDescriptor::debug_string() const {
    std::stringstream out;
    out << "Slot(id=" << _id << " type=" << _type << " col=" << _col_pos
        << ", colname=" << _col_name << ", nullable=" << is_nullable() << ")";
    return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
        : _table_type(tdesc.tableType),
          _name(tdesc.tableName),
          _database(tdesc.dbName),
          _table_id(tdesc.id),
          _num_cols(tdesc.numCols),
          _num_clustering_cols(tdesc.numClusteringCols) {}

std::string TableDescriptor::debug_string() const {
    std::stringstream out;
    out << "#cols=" << _num_cols << " #clustering_cols=" << _num_clustering_cols;
    return out.str();
}

OlapTableDescriptor::OlapTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

std::string OlapTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "OlapTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

SchemaTableDescriptor::SchemaTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc), _schema_table_type(tdesc.schemaTable.tableType) {}
SchemaTableDescriptor::~SchemaTableDescriptor() = default;

std::string SchemaTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "SchemaTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

BrokerTableDescriptor::BrokerTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

BrokerTableDescriptor::~BrokerTableDescriptor() = default;

std::string BrokerTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "BrokerTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

HiveTableDescriptor::HiveTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

HiveTableDescriptor::~HiveTableDescriptor() = default;

std::string HiveTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "HiveTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

IcebergTableDescriptor::IcebergTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

IcebergTableDescriptor::~IcebergTableDescriptor() = default;

std::string IcebergTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "IcebergTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

MaxComputeTableDescriptor::MaxComputeTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _region(tdesc.mcTable.region),
          _project(tdesc.mcTable.project),
          _table(tdesc.mcTable.table),
          _odps_url(tdesc.mcTable.odps_url),
          _tunnel_url(tdesc.mcTable.tunnel_url),
          _access_key(tdesc.mcTable.access_key),
          _secret_key(tdesc.mcTable.secret_key),
          _public_access(tdesc.mcTable.public_access) {}

MaxComputeTableDescriptor::~MaxComputeTableDescriptor() = default;

std::string MaxComputeTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "MaxComputeTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

TrinoConnectorTableDescriptor::TrinoConnectorTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

TrinoConnectorTableDescriptor::~TrinoConnectorTableDescriptor() = default;

std::string TrinoConnectorTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "TrinoConnectorTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

EsTableDescriptor::EsTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

EsTableDescriptor::~EsTableDescriptor() = default;

std::string EsTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "EsTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

MySQLTableDescriptor::MySQLTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _mysql_db(tdesc.mysqlTable.db),
          _mysql_table(tdesc.mysqlTable.table),
          _host(tdesc.mysqlTable.host),
          _port(tdesc.mysqlTable.port),
          _user(tdesc.mysqlTable.user),
          _passwd(tdesc.mysqlTable.passwd),
          _charset(tdesc.mysqlTable.charset) {}

std::string MySQLTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "MySQLTable(" << TableDescriptor::debug_string() << " _db" << _mysql_db
        << " table=" << _mysql_table << " host=" << _host << " port=" << _port << " user=" << _user
        << " passwd=" << _passwd << " charset=" << _charset;
    return out.str();
}

ODBCTableDescriptor::ODBCTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _db(tdesc.odbcTable.db),
          _table(tdesc.odbcTable.table),
          _host(tdesc.odbcTable.host),
          _port(tdesc.odbcTable.port),
          _user(tdesc.odbcTable.user),
          _passwd(tdesc.odbcTable.passwd),
          _driver(tdesc.odbcTable.driver),
          _type(tdesc.odbcTable.type) {}

std::string ODBCTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "ODBCTable(" << TableDescriptor::debug_string() << " _db" << _db << " table=" << _table
        << " host=" << _host << " port=" << _port << " user=" << _user << " passwd=" << _passwd
        << " driver=" << _driver << " type" << _type;
    return out.str();
}

JdbcTableDescriptor::JdbcTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _jdbc_catalog_id(tdesc.jdbcTable.catalog_id),
          _jdbc_resource_name(tdesc.jdbcTable.jdbc_resource_name),
          _jdbc_driver_url(tdesc.jdbcTable.jdbc_driver_url),
          _jdbc_driver_class(tdesc.jdbcTable.jdbc_driver_class),
          _jdbc_driver_checksum(tdesc.jdbcTable.jdbc_driver_checksum),
          _jdbc_url(tdesc.jdbcTable.jdbc_url),
          _jdbc_table_name(tdesc.jdbcTable.jdbc_table_name),
          _jdbc_user(tdesc.jdbcTable.jdbc_user),
          _jdbc_passwd(tdesc.jdbcTable.jdbc_password),
          _connection_pool_min_size(tdesc.jdbcTable.connection_pool_min_size),
          _connection_pool_max_size(tdesc.jdbcTable.connection_pool_max_size),
          _connection_pool_max_wait_time(tdesc.jdbcTable.connection_pool_max_wait_time),
          _connection_pool_max_life_time(tdesc.jdbcTable.connection_pool_max_life_time),
          _connection_pool_keep_alive(tdesc.jdbcTable.connection_pool_keep_alive) {}

std::string JdbcTableDescriptor::debug_string() const {
    fmt::memory_buffer buf;
    fmt::format_to(
            buf,
            "JDBCTable({} ,_jdbc_catalog_id = {}, _jdbc_resource_name={} ,_jdbc_driver_url={} "
            ",_jdbc_driver_class={} ,_jdbc_driver_checksum={} ,_jdbc_url={} "
            ",_jdbc_table_name={} ,_jdbc_user={} ,_jdbc_passwd={} ,_connection_pool_min_size={} "
            ",_connection_pool_max_size={} ,_connection_pool_max_wait_time={} "
            ",_connection_pool_max_life_time={} ,_connection_pool_keep_alive={})",
            TableDescriptor::debug_string(), _jdbc_catalog_id, _jdbc_resource_name,
            _jdbc_driver_url, _jdbc_driver_class, _jdbc_driver_checksum, _jdbc_url,
            _jdbc_table_name, _jdbc_user, _jdbc_passwd, _connection_pool_min_size,
            _connection_pool_max_size, _connection_pool_max_wait_time,
            _connection_pool_max_life_time, _connection_pool_keep_alive);
    return fmt::to_string(buf);
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc, bool own_slots)
        : _id(tdesc.id),
          _num_materialized_slots(0),
          _has_varlen_slots(false),
          _own_slots(own_slots) {}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc, bool own_slots)
        : _id(pdesc.id()),
          _num_materialized_slots(0),
          _has_varlen_slots(false),
          _own_slots(own_slots) {}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);

    if (slot->is_materialized()) {
        ++_num_materialized_slots;

        if (slot->type().is_string_type() || slot->type().is_complex_type()) {
            _has_varlen_slots = true;
        }
    }
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    // Useless not set
    ptuple->set_byte_size(0);
    ptuple->set_table_id(-1);
    ptuple->set_num_null_bytes(0);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id;
    if (_table_desc != nullptr) {
        //out << " " << _table_desc->debug_string();
    }

    out << " slots=[";
    for (size_t i = 0; i < _slots.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << _slots[i]->debug_string();
    }

    out << "]";
    out << " has_varlen_slots=" << _has_varlen_slots;
    out << ")";
    return out.str();
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
                             const std::vector<bool>& nullable_tuples)
        : _tuple_idx_nullable_map(nullable_tuples) {
    DCHECK(nullable_tuples.size() == row_tuples.size())
            << "nullable_tuples size " << nullable_tuples.size() << " != row_tuples size "
            << row_tuples.size();
    DCHECK_GT(row_tuples.size(), 0);
    _num_materialized_slots = 0;

    for (int row_tuple : row_tuples) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuple);
        _num_materialized_slots += tupleDesc->num_materialized_slots();
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != nullptr);
    }

    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable)
        : _tuple_desc_map(1, tuple_desc), _tuple_idx_nullable_map(1, is_nullable) {
    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc) {
    _tuple_desc_map.insert(_tuple_desc_map.end(), lhs_row_desc._tuple_desc_map.begin(),
                           lhs_row_desc._tuple_desc_map.end());
    _tuple_desc_map.insert(_tuple_desc_map.end(), rhs_row_desc._tuple_desc_map.begin(),
                           rhs_row_desc._tuple_desc_map.end());
    _tuple_idx_nullable_map.insert(_tuple_idx_nullable_map.end(),
                                   lhs_row_desc._tuple_idx_nullable_map.begin(),
                                   lhs_row_desc._tuple_idx_nullable_map.end());
    _tuple_idx_nullable_map.insert(_tuple_idx_nullable_map.end(),
                                   rhs_row_desc._tuple_idx_nullable_map.begin(),
                                   rhs_row_desc._tuple_idx_nullable_map.end());
    init_tuple_idx_map();
    init_has_varlen_slots();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (auto& i : _tuple_desc_map) {
        max_id = std::max(i->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

void RowDescriptor::init_has_varlen_slots() {
    _has_varlen_slots = false;
    for (auto& i : _tuple_desc_map) {
        if (i->has_varlen_slots()) {
            _has_varlen_slots = true;
            break;
        }
    }
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    // comment CHECK temporarily to make fuzzy test run smoothly
    // DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    if (_tuple_idx_map.size() <= id) {
        return RowDescriptor::INVALID_IDX;
    }
    return _tuple_idx_map[id];
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (auto& i : _tuple_desc_map) {
        row_tuple_ids->push_back(i->id());
    }
}

void RowDescriptor::to_protobuf(
        google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) const {
    row_tuple_ids->Clear();
    for (auto* desc : _tuple_desc_map) {
        row_tuple_ids->Add(desc->id());
    }
}

bool RowDescriptor::is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

std::string RowDescriptor::debug_string() const {
    std::stringstream ss;

    ss << "tuple_desc_map: [";
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        ss << _tuple_desc_map[i]->debug_string();
        if (i != _tuple_desc_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_id_map: [";
    for (int i = 0; i < _tuple_idx_map.size(); ++i) {
        ss << _tuple_idx_map[i];
        if (i != _tuple_idx_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_is_nullable: [";
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        ss << _tuple_idx_nullable_map[i];
        if (i != _tuple_idx_nullable_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    return ss.str();
}

int RowDescriptor::get_column_id(int slot_id, bool force_materialize_slot) const {
    int column_id_counter = 0;
    for (auto* const tuple_desc : _tuple_desc_map) {
        for (auto* const slot : tuple_desc->slots()) {
            if (!force_materialize_slot && !slot->need_materialize()) {
                continue;
            }
            if (slot->id() == slot_id) {
                return column_id_counter;
            }
            column_id_counter++;
        }
    }
    return -1;
}

Status DescriptorTbl::create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
    *tbl = pool->add(new DescriptorTbl());

    // deserialize table descriptors first, they are being referenced by tuple descriptors
    for (const auto& tdesc : thrift_tbl.tableDescriptors) {
        TableDescriptor* desc = nullptr;

        switch (tdesc.tableType) {
        case TTableType::MYSQL_TABLE:
            desc = pool->add(new MySQLTableDescriptor(tdesc));
            break;

        case TTableType::ODBC_TABLE:
            desc = pool->add(new ODBCTableDescriptor(tdesc));
            break;

        case TTableType::OLAP_TABLE:
            desc = pool->add(new OlapTableDescriptor(tdesc));
            break;

        case TTableType::SCHEMA_TABLE:
            desc = pool->add(new SchemaTableDescriptor(tdesc));
            break;
        case TTableType::BROKER_TABLE:
            desc = pool->add(new BrokerTableDescriptor(tdesc));
            break;
        case TTableType::ES_TABLE:
            desc = pool->add(new EsTableDescriptor(tdesc));
            break;
        case TTableType::HIVE_TABLE:
            desc = pool->add(new HiveTableDescriptor(tdesc));
            break;
        case TTableType::ICEBERG_TABLE:
            desc = pool->add(new IcebergTableDescriptor(tdesc));
            break;
        case TTableType::JDBC_TABLE:
            desc = pool->add(new JdbcTableDescriptor(tdesc));
            break;
        case TTableType::MAX_COMPUTE_TABLE:
            desc = pool->add(new MaxComputeTableDescriptor(tdesc));
            break;
        case TTableType::TRINO_CONNECTOR_TABLE:
            desc = pool->add(new TrinoConnectorTableDescriptor(tdesc));
            break;
        default:
            DCHECK(false) << "invalid table type: " << tdesc.tableType;
        }

        (*tbl)->_tbl_desc_map[tdesc.id] = desc;
    }

    for (const auto& tdesc : thrift_tbl.tupleDescriptors) {
        TupleDescriptor* desc = pool->add(new TupleDescriptor(tdesc));

        // fix up table pointer
        if (tdesc.__isset.tableId) {
            desc->_table_desc = (*tbl)->get_table_descriptor(tdesc.tableId);
            DCHECK(desc->_table_desc != nullptr);
        }

        (*tbl)->_tuple_desc_map[tdesc.id] = desc;
        (*tbl)->_row_tuples.emplace_back(tdesc.id);
    }

    for (const auto& tdesc : thrift_tbl.slotDescriptors) {
        SlotDescriptor* slot_d = pool->add(new SlotDescriptor(tdesc));
        (*tbl)->_slot_desc_map[tdesc.id] = slot_d;

        // link to parent
        auto entry = (*tbl)->_tuple_desc_map.find(tdesc.parent);

        if (entry == (*tbl)->_tuple_desc_map.end()) {
            return Status::InternalError("unknown tid in slot descriptor msg");
        }
        entry->second->add_slot(slot_d);
    }

    return Status::OK();
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _tbl_desc_map.find(id);

    if (i == _tbl_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _tuple_desc_map.find(id);

    if (i == _tuple_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (auto i : _tuple_desc_map) {
        out << i.second->debug_string() << '\n';
    }

    return out.str();
}

} // namespace doris
