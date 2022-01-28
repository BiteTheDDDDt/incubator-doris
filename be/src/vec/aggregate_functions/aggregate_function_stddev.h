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

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
namespace doris::vectorized {

template <typename T, bool is_stddev>
struct BaseData {
    BaseData() : mean(0.0), m2(0.0), count(0) {}

    void write(BufferWritable& buf) const {
        write_binary(mean, buf);
        write_binary(m2, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(mean, buf);
        read_binary(m2, buf);
        read_binary(count, buf);
    }

    void reset() {
        mean = 0.0;
        m2 = 0.0;
        count = 0;
    }

    double get_result(double res) const {
        if constexpr (is_stddev) {
            return std::sqrt(res);
        } else {
            return res;
        }
    }

    double get_pop_result() const {
        if (count == 1) {
            return 0.0;
        }
        double res = m2 / count;
        return get_result(res);
    }

    double get_samp_result() const {
        double res = m2 / (count - 1);
        return get_result(res);
    }

    static const DataTypePtr get_return_type() {
        return make_nullable(std::make_shared<DataTypeNumber<Float64>>());
    }

    void merge(const BaseData& rhs) {
        if (rhs.count == 0) {
            return;
        }
        double delta = mean - rhs.mean;
        double sum_count = count + rhs.count;
        mean = rhs.mean + delta * count / sum_count;
        m2 = rhs.m2 + m2 + (delta * delta) * rhs.count * count / sum_count;
        count = sum_count;
    }

    void add(const IColumn** columns, size_t row_num) {
        const auto& sources = static_cast<const ColumnVector<T>&>(*columns[0]);
        double source_data = sources.get_data()[row_num];

        double delta = source_data - mean;
        double r = delta / (1 + count);
        mean += r;
        m2 += count * delta * r;
        count += 1;
    }

    double mean;
    double m2;
    int64_t count;
};

template <bool is_stddev>
struct BaseDatadecimal {
    BaseDatadecimal() : mean(0), m2(0), count(0) {}

    void write(BufferWritable& buf) const {
        write_binary(mean, buf);
        write_binary(m2, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(mean, buf);
        read_binary(m2, buf);
        read_binary(count, buf);
    }

    void reset() {
        mean = DecimalV2Value();
        m2 = DecimalV2Value();
        count = {};
    }

    DecimalV2Value get_result(DecimalV2Value res) const {
        if constexpr (is_stddev) {
            return DecimalV2Value::sqrt(res);
        } else {
            return res;
        }
    }

    DecimalV2Value get_pop_result() const {
        DecimalV2Value new_count = DecimalV2Value();
        if (count == 1) {
            return new_count;
        }
        DecimalV2Value res = m2 / new_count.assign_from_double(count);
        return get_result(res);
    }

    DecimalV2Value get_samp_result() const {
        DecimalV2Value new_count = DecimalV2Value();
        DecimalV2Value res = m2 / new_count.assign_from_double(count - 1);
        return get_result(res);
    }

    static const DataTypePtr get_return_type() {
        return make_nullable(std::make_shared<DataTypeDecimal<Decimal128>>(27, 9));
    }

    void merge(const BaseDatadecimal& rhs) {
        if (rhs.count == 0) {
            return;
        }
        DecimalV2Value new_count = DecimalV2Value();
        new_count.assign_from_double(count);
        DecimalV2Value rhs_count = DecimalV2Value();
        rhs_count.assign_from_double(rhs.count);

        DecimalV2Value delta = mean - rhs.mean;
        DecimalV2Value sum_count = new_count + rhs_count;
        mean = rhs.mean + delta * (new_count / sum_count);
        m2 = rhs.m2 + m2 + (delta * delta) * (rhs_count * new_count / sum_count);
        count += rhs.count;
    }

    void add(const IColumn** columns, size_t row_num) {
        DecimalV2Value source_data = DecimalV2Value();
        const auto& sources = static_cast<const ColumnDecimal<Decimal128>&>(*columns[0]);
        source_data = (DecimalV2Value)sources.get_data()[row_num];

        DecimalV2Value new_count = DecimalV2Value();
        new_count.assign_from_double(count);
        DecimalV2Value increase_count = DecimalV2Value();
        increase_count.assign_from_double(1 + count);

        DecimalV2Value delta = source_data - mean;
        DecimalV2Value r = delta / increase_count;
        mean += r;
        m2 += new_count * delta * r;
        count += 1;
    }

    DecimalV2Value mean;
    DecimalV2Value m2;
    int64_t count;
};

template <typename T, typename Data>
struct PopData : Data {
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>,
                                            ColumnVector<Float64>>;
    void insert_result_into(IColumn& to) const {
        ColumnNullable& nullable_column = assert_cast<ColumnNullable&>(to);
        auto& col = static_cast<ColVecResult&>(nullable_column.get_nested_column());
        if constexpr (IsDecimalNumber<T>) {
            col.get_data().push_back(this->get_pop_result().value());
        } else {
            col.get_data().push_back(this->get_pop_result());
        }
        nullable_column.get_null_map_data().push_back(0);
    }
};

template <typename T, typename Data>
struct SampData : Data {
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>,
                                            ColumnVector<Float64>>;
    void insert_result_into(IColumn& to) const {
        ColumnNullable& nullable_column = assert_cast<ColumnNullable&>(to);
        if (this->count == 1) {
            nullable_column.insert_default();
        } else {
            auto& col = static_cast<ColVecResult&>(nullable_column.get_nested_column());
            if constexpr (IsDecimalNumber<T>) {
                col.get_data().push_back(this->get_samp_result().value());
            } else {
                col.get_data().push_back(this->get_samp_result());
            }
            nullable_column.get_null_map_data().push_back(0);
        }
    }
};

template <typename Data>
struct StddevData : Data {
    static const char* name() { return "stddev"; }
};

template <typename Data>
struct VarianceData : Data {
    static const char* name() { return "variance"; }
};

template <typename Data>
struct VarianceSampData : Data {
    static const char* name() { return "variance_samp"; }
};

template <typename Data>
struct StddevSampData : Data {
    static const char* name() { return "stddev_samp"; }
};

template <typename Data>
class AggregateFunctionStddevSamp final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionStddevSamp<Data>> {
public:
    AggregateFunctionStddevSamp(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionStddevSamp<Data>>(argument_types_,
                                                                                    {}) {}

    String get_name() const override { return Data::name(); }

    bool insert_to_null_default() const override { return false; }

    DataTypePtr get_return_type() const override { return Data::get_return_type(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        this->data(place).add(columns, row_num);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    const char* get_header_file_path() const override { return __FILE__; }
};

} // namespace doris::vectorized
