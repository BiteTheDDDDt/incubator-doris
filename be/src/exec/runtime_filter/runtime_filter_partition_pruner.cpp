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

#include "exec/runtime_filter/runtime_filter_partition_pruner.h"

#include <gen_cpp/PlanNodes_types.h>

#include <optional>
#include <utility>

#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"

namespace doris {

// Helper to extract a typed value from a ColumnPtr's first row.
// Must be a template function so that `if constexpr` properly discards
// the inapplicable branch during instantiation.
template <PrimitiveType PT>
static typename PrimitiveTypeTraits<PT>::CppType extract_value_from_column(const ColumnPtr& col) {
    using CppType = typename PrimitiveTypeTraits<PT>::CppType;
    auto data = col->get_data_at(0);
    if constexpr (is_string_type(PT)) {
        return CppType(data.data, data.size);
    } else {
        return *reinterpret_cast<const CppType*>(data.data);
    }
}

// NOLINTBEGIN(readability-function-cognitive-complexity,readability-function-size)
// Complexity is inflated by macro expansion for each PrimitiveType case.
void RuntimeFilterPartitionPruner::parse_boundaries(
        const std::vector<TPartitionBoundary>& boundaries,
        const phmap::flat_hash_map<int, SlotDescriptor*>& slot_descs) {
    for (const auto& tb : boundaries) {
        if (!tb.__isset.partition_id || !tb.__isset.slot_id) {
            continue;
        }
        SlotId slot_id = tb.slot_id;

        auto slot_it = slot_descs.find(slot_id);
        if (slot_it == slot_descs.end()) {
            continue;
        }
        SlotDescriptor* slot = slot_it->second;
        PrimitiveType ptype = slot->type()->get_primitive_type();
        int precision = cast_set<int>(slot->type()->get_precision());
        int scale = cast_set<int>(slot->type()->get_scale());
        bool is_nullable = slot->is_nullable();

        ParsedBoundary boundary;
        boundary.partition_id = tb.partition_id;
        boundary.slot_id = slot_id;
        boundary.is_nullable = is_nullable;

        bool parsed_ok = false;

#define BUILD_BOUNDARY_CVR(NAME)                                                               \
    case TYPE_##NAME: {                                                                        \
        using CppType = typename PrimitiveTypeTraits<TYPE_##NAME>::CppType;                    \
        bool is_list = tb.__isset.list_values && !tb.list_values.empty();                      \
        bool is_range = tb.__isset.range_start || tb.__isset.range_end;                        \
        if (!is_list && !is_range) break;                                                      \
        ColumnValueRange<TYPE_##NAME> cvr(slot->col_name(), is_nullable, precision, scale);    \
        /* Returns nullopt if `node` is a NULL literal; the caller then sets contain_null  */  \
        /* on the CVR instead of trying to extract a typed value (which would dereference  */  \
        /* a null data pointer for the non-string branch).                                 */  \
        auto parse_texpr_node =                                                                \
                [&](const TExprNode& node) -> std::optional<std::pair<CppType, ColumnPtr>> {   \
            if (node.node_type == TExprNodeType::NULL_LITERAL) {                               \
                return std::nullopt;                                                           \
            }                                                                                  \
            VLiteral literal(node);                                                            \
            auto col_ptr = literal.get_column_ptr();                                           \
            auto val = extract_value_from_column<TYPE_##NAME>(col_ptr);                        \
            return std::make_optional(std::pair<CppType, ColumnPtr> {val, col_ptr});           \
        };                                                                                     \
        if (is_list) {                                                                         \
            auto empty_cvr = ColumnValueRange<TYPE_##NAME>::create_empty_column_value_range(   \
                    is_nullable, precision, scale);                                            \
            bool list_has_null = false;                                                        \
            bool list_has_value = false;                                                       \
            for (const auto& node : tb.list_values) {                                          \
                auto parsed = parse_texpr_node(node);                                          \
                if (!parsed) {                                                                 \
                    list_has_null = true;                                                      \
                    continue;                                                                  \
                }                                                                              \
                auto& [val, col_ptr] = *parsed;                                                \
                boundary.literal_columns.push_back(std::move(col_ptr));                        \
                static_cast<void>(empty_cvr.add_fixed_value(val));                             \
                list_has_value = true;                                                         \
            }                                                                                  \
            if (list_has_value) {                                                              \
                cvr.intersection(empty_cvr);                                                   \
            }                                                                                  \
            if (list_has_null && is_nullable) {                                                \
                cvr.set_contain_null(true);                                                    \
                if (!list_has_value) {                                                         \
                    boundary.only_null = true;                                                 \
                }                                                                              \
            }                                                                                  \
        } else {                                                                               \
            if (tb.__isset.range_start) {                                                      \
                auto parsed = parse_texpr_node(tb.range_start);                                \
                if (parsed) {                                                                  \
                    auto& [val, col_ptr] = *parsed;                                            \
                    boundary.literal_columns.push_back(std::move(col_ptr));                    \
                    static_cast<void>(cvr.add_range(FILTER_LARGER_OR_EQUAL, val));             \
                }                                                                              \
            }                                                                                  \
            if (tb.__isset.range_end) {                                                        \
                auto parsed = parse_texpr_node(tb.range_end);                                  \
                if (parsed) {                                                                  \
                    auto& [val, col_ptr] = *parsed;                                            \
                    boundary.literal_columns.push_back(std::move(col_ptr));                    \
                    /* Multi-column RANGE projection emits a CLOSED upper bound (see       */  \
                    /* TPartitionBoundary.range_end_inclusive comment); single-column RANGE */ \
                    /* keeps the natural OPEN upper bound matching Doris semantics.         */ \
                    SQLFilterOp upper_op =                                                     \
                            (tb.__isset.range_end_inclusive && tb.range_end_inclusive)         \
                                    ? FILTER_LESS_OR_EQUAL                                     \
                                    : FILTER_LESS;                                             \
                    static_cast<void>(cvr.add_range(upper_op, val));                           \
                }                                                                              \
            }                                                                                  \
        }                                                                                      \
        boundary.boundary_cvr = std::move(cvr);                                                \
        parsed_ok = true;                                                                      \
        break;                                                                                 \
    }

        switch (ptype) {
            BUILD_BOUNDARY_CVR(TINYINT)
            BUILD_BOUNDARY_CVR(SMALLINT)
            BUILD_BOUNDARY_CVR(INT)
            BUILD_BOUNDARY_CVR(BIGINT)
            BUILD_BOUNDARY_CVR(LARGEINT)
            BUILD_BOUNDARY_CVR(FLOAT)
            BUILD_BOUNDARY_CVR(DOUBLE)
            BUILD_BOUNDARY_CVR(CHAR)
            BUILD_BOUNDARY_CVR(DATE)
            BUILD_BOUNDARY_CVR(DATETIME)
            BUILD_BOUNDARY_CVR(DATEV2)
            BUILD_BOUNDARY_CVR(DATETIMEV2)
            BUILD_BOUNDARY_CVR(TIMESTAMPTZ)
            BUILD_BOUNDARY_CVR(VARCHAR)
            BUILD_BOUNDARY_CVR(STRING)
            BUILD_BOUNDARY_CVR(DECIMAL32)
            BUILD_BOUNDARY_CVR(DECIMAL64)
            BUILD_BOUNDARY_CVR(DECIMAL128I)
            BUILD_BOUNDARY_CVR(DECIMAL256)
            BUILD_BOUNDARY_CVR(DECIMALV2)
            BUILD_BOUNDARY_CVR(BOOLEAN)
            BUILD_BOUNDARY_CVR(IPV4)
            BUILD_BOUNDARY_CVR(IPV6)
        default:
            break;
        }
#undef BUILD_BOUNDARY_CVR

        if (parsed_ok) {
            _partition_column_slot_ids.insert(slot_id);
            _slot_to_boundaries[slot_id].push_back(std::move(boundary));
        }
    }

    // Count distinct partition IDs across all boundaries.
    if (!_partition_column_slot_ids.empty()) {
        phmap::flat_hash_set<int64_t> all_partition_ids;
        for (const auto& [_, slot_boundaries] : _slot_to_boundaries) {
            for (const auto& pb : slot_boundaries) {
                all_partition_ids.insert(pb.partition_id);
            }
        }
        _total_partition_count = static_cast<int64_t>(all_partition_ids.size());
    }
}
// NOLINTEND(readability-function-cognitive-complexity,readability-function-size)

static SQLFilterOp convert_opcode_to_filter_op(TExprOpcode::type op) {
    switch (op) {
    case TExprOpcode::LE:
        return FILTER_LESS_OR_EQUAL;
    case TExprOpcode::LT:
        return FILTER_LESS;
    case TExprOpcode::GE:
        return FILTER_LARGER_OR_EQUAL;
    case TExprOpcode::GT:
        return FILTER_LARGER;
    default:
        return FILTER_IN; // sentinel: caller should skip
    }
}

void RuntimeFilterPartitionPruner::_try_prune_by_single_rf(
        const VExprSPtr& impl, SlotId slot_id, phmap::flat_hash_set<int64_t>& newly_pruned) {
    auto boundaries_it = _slot_to_boundaries.find(slot_id);
    if (boundaries_it == _slot_to_boundaries.end()) {
        return;
    }

    // Pre-compute whether the RF "matches NULL" -- i.e. whether the RF would
    // accept a row whose probe value is NULL. This is encoded by the set's
    // contain_null() (see FilterBase::contain_null = _null_aware && _contain_null).
    // For BINARY_PRED predicates (=, <, >, <=, >=) NULL never compares true,
    // so they never match a NULL probe row -- treat as not containing NULL.
    auto hybrid_set_for_null = impl->get_set_func();
    bool rf_contains_null = hybrid_set_for_null && hybrid_set_for_null->contain_null();

    for (const auto& pb : boundaries_it->second) {
        if (_pruned_partition_ids.contains(pb.partition_id) ||
            newly_pruned.contains(pb.partition_id)) {
            continue;
        }

        // NULL handling:
        //   A partition row whose key is NULL matches the RF iff `rf_contains_null`.
        //   - only_null partition (rows are exclusively NULL): prunable iff !rf_contains_null.
        //   - mixed (NULL + concrete values): if rf_contains_null, NULL rows alone
        //     prevent pruning. Otherwise NULL rows can never match, so we ignore
        //     contain_null and fall through to the regular non-NULL intersection.
        bool partition_contains_null =
                std::visit([](const auto& cvr) { return cvr.contain_null(); }, pb.boundary_cvr);
        if (pb.only_null) {
            if (!rf_contains_null) {
                newly_pruned.insert(pb.partition_id);
            }
            continue;
        }
        if (partition_contains_null && rf_contains_null) {
            continue;
        }

        std::visit(
                [&](const auto& boundary_cvr) {
                    using CvrType = std::decay_t<decltype(boundary_cvr)>;
                    using CppType = typename CvrType::CppType;

                    auto hybrid_set = impl->get_set_func();
                    if (hybrid_set) {
                        // IN filter: build a fixed-value CVR from the HybridSet
                        auto rf_cvr = CvrType::create_empty_column_value_range(
                                pb.is_nullable, boundary_cvr.precision(), boundary_cvr.scale());
                        auto* iter = hybrid_set->begin();
                        while (iter->has_next()) {
                            const void* value = iter->get_value();
                            if (value) {
                                if constexpr (std::is_same_v<CppType, StringRef>) {
                                    const auto* str_val = reinterpret_cast<const StringRef*>(value);
                                    static_cast<void>(rf_cvr.add_fixed_value(
                                            CppType(str_val->data, str_val->size)));
                                } else {
                                    static_cast<void>(rf_cvr.add_fixed_value(
                                            *reinterpret_cast<const CppType*>(value)));
                                }
                            }
                            iter->next();
                        }
                        auto boundary_copy = boundary_cvr;
                        boundary_copy.intersection(rf_cvr);
                        if (boundary_copy.is_empty_value_range()) {
                            newly_pruned.insert(pb.partition_id);
                        }
                    } else if (impl->node_type() == TExprNodeType::BINARY_PRED &&
                               impl->children().size() == 2 && impl->children()[1]->is_literal()) {
                        // MinMax filter: binary pred with literal bound
                        auto* literal = assert_cast<VLiteral*>(impl->children()[1].get());
                        auto col_ptr = literal->get_column_ptr();
                        auto data = col_ptr->get_data_at(0);
                        CppType val {};
                        if constexpr (std::is_same_v<CppType, StringRef>) {
                            val = CppType(data.data, data.size);
                        } else {
                            val = *reinterpret_cast<const CppType*>(data.data);
                        }

                        SQLFilterOp op = convert_opcode_to_filter_op(impl->op());
                        if (op == FILTER_IN) {
                            return; // unrecognized opcode, skip
                        }

                        CvrType rf_cvr(boundary_cvr.column_name(), pb.is_nullable,
                                       boundary_cvr.precision(), boundary_cvr.scale());
                        static_cast<void>(rf_cvr.add_range(op, val));

                        auto boundary_copy = boundary_cvr;
                        boundary_copy.intersection(rf_cvr);
                        if (boundary_copy.is_empty_value_range()) {
                            newly_pruned.insert(pb.partition_id);
                        }
                    }
                },
                pb.boundary_cvr);
    }
}

int64_t RuntimeFilterPartitionPruner::prune_by_runtime_filters(const VExprContextSPtrs& conjuncts) {
    if (_partition_column_slot_ids.empty()) {
        return 0;
    }

    // This function is serialized by _conjuncts_lock in the caller, so our reads
    // of _pruned_partition_ids never race with our writes below. The only concurrent
    // readers are is_partition_pruned() calls (under shared_lock), which are
    // properly synchronized by the unique_lock we take when inserting.
    phmap::flat_hash_set<int64_t> newly_pruned;

    for (const auto& conjunct_ctx : conjuncts) {
        VExprSPtr root = conjunct_ctx->root();
        if (!root->is_rf_wrapper()) {
            continue;
        }

        VExprSPtr impl = root->get_impl();
        if (!impl) {
            continue;
        }

        // Only handle RFs whose target is a simple SlotRef on a partition column.
        if (impl->children().empty() || !impl->children()[0]->is_slot_ref()) {
            continue;
        }
        auto* slot_ref = assert_cast<VSlotRef*>(impl->children()[0].get());
        SlotId slot_id = slot_ref->slot_id();
        if (!_partition_column_slot_ids.contains(slot_id)) {
            continue;
        }

        _try_prune_by_single_rf(impl, slot_id, newly_pruned);
    }

    auto count = static_cast<int64_t>(newly_pruned.size());
    if (count > 0) {
        std::unique_lock lock(_prune_mutex);
        for (int64_t pid : newly_pruned) {
            _pruned_partition_ids.insert(pid);
        }
    }
    return count;
}

bool RuntimeFilterPartitionPruner::is_partition_pruned(int64_t partition_id) const {
    std::shared_lock lock(_prune_mutex);
    return _pruned_partition_ids.contains(partition_id);
}

int64_t RuntimeFilterPartitionPruner::pruned_partition_count() const {
    std::shared_lock lock(_prune_mutex);
    return static_cast<int64_t>(_pruned_partition_ids.size());
}

} // namespace doris
