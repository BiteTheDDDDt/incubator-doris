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

#include "vec/aggregate_functions/aggregate_function_percentile_approx.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_percentile_approx(const std::string& name,
                                                                 const DataTypes& argument_types,
                                                                 const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        return creator_without_type::create<AggregateFunctionPercentileApproxMerge<is_nullable>>(
                result_is_nullable, remove_nullable(argument_types));
    } else if (argument_types.size() == 2) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxTwoParams<is_nullable>>(
                result_is_nullable, remove_nullable(argument_types));
    } else if (argument_types.size() == 3) {
        return creator_without_type::create<
                AggregateFunctionPercentileApproxThreeParams<is_nullable>>(
                result_is_nullable, remove_nullable(argument_types));
    }
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_percentile(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    return creator_without_type::create<AggregateFunctionPercentile>(result_is_nullable,
                                                                     argument_types);
}

AggregateFunctionPtr create_aggregate_function_percentile_array(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable) {
    return creator_without_type::create<AggregateFunctionPercentileArray>(result_is_nullable,
                                                                          argument_types);
}

void register_aggregate_function_percentile(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("percentile", create_aggregate_function_percentile);
    factory.register_function_both("percentile_array", create_aggregate_function_percentile_array);
}

void register_aggregate_function_percentile_approx(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("percentile_approx",
                                   create_aggregate_function_percentile_approx<false>);
    factory.register_function_both("percentile_approx",
                                   create_aggregate_function_percentile_approx<true>);
}
} // namespace doris::vectorized