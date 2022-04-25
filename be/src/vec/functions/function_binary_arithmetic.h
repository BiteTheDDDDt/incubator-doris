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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionBinaryArithmetic.h
// and modified by Doris

#pragma once

#include "runtime/tuple.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

// Arithmetic operations: +, -, *, |, &, ^, ~
// Arithmetic operations (to null type): /, %, intDiv (integer division), log

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImplBase {
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ColumnVectorResult =
            std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>,
                               ColumnVector<ResultType>>;

    static void vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                              PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                              PaddedPODArray<ResultType>& c, PaddedPODArray<UInt8>& null_map) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i], null_map[i]);
        }
    }

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b);
        }
    }

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c,
                                PaddedPODArray<UInt8>& null_map) {
        Op::template apply<ResultType>(a, b, c, null_map);
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<ResultType>& c) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i]);
        }
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<ResultType>& c,
                                PaddedPODArray<UInt8>& null_map) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i], null_map[i]);
        }
    }

    static ResultType constant_constant(A a, B b) { return Op::template apply<ResultType>(a, b); }

    static ResultType constant_constant(A a, B b, UInt8& is_null) {
        return Op::template apply<ResultType>(a, b, is_null);
    }
};

template <typename A, typename B, typename Op, bool is_to_null_type,
          typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl {
    using Base = BinaryOperationImplBase<A, B, Op, ResultType>;

    static ColumnPtr adapt_normal_constant_constant(A a, B b) {
        auto column_result = Base::ColumnVectorResult::create(1);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(1, 0);
            column_result->get_element(0) = Base::constant_constant(a, b, null_map->get_element(0));
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            column_result->get_element(0) = Base::constant_constant(a, b);
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_vector_constant(ColumnPtr column_left, B b) {
        auto column_left_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left);
        auto column_result = Base::ColumnVectorResult::create(column_left->size());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            Base::vector_constant(column_left_ptr->get_data(), b, column_result->get_data(),
                                  null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::vector_constant(column_left_ptr->get_data(), b, column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_constant_vector(A a, ColumnPtr column_right) {
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right);
        auto column_result = Base::ColumnVectorResult::create(column_right->size());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            Base::constant_vector(a, column_right_ptr->get_data(), column_result->get_data(),
                                  null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::constant_vector(a, column_right_ptr->get_data(), column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_vector_vector(ColumnPtr column_left, ColumnPtr column_right) {
        auto column_left_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left);
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right);

        auto column_result = Base::ColumnVectorResult::create(column_left->size());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            Base::vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                                column_result->get_data(), null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                                column_result->get_data());
            return column_result;
        }
    }
};

template <typename, typename>
struct PlusImpl;
template <typename, typename>
struct MinusImpl;
template <typename, typename>
struct MultiplyImpl;
template <typename, typename>
struct DivideFloatingImpl;
template <typename, typename>
struct DivideIntegralImpl;
template <typename, typename>
struct ModuloImpl;

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::get_scale()).
template <typename A, typename B, template <typename, typename> typename Operation,
          typename ResultType, bool is_to_null_type, bool check_overflow = false>
struct DecimalBinaryOperation {
    static constexpr bool is_plus_minus =
            std::is_same_v<Operation<Int32, Int32>, PlusImpl<Int32, Int32>> ||
            std::is_same_v<Operation<Int32, Int32>, MinusImpl<Int32, Int32>>;
    static constexpr bool is_multiply =
            std::is_same_v<Operation<Int32, Int32>, MultiplyImpl<Int32, Int32>>;
    static constexpr bool is_float_division =
            std::is_same_v<Operation<Int32, Int32>, DivideFloatingImpl<Int32, Int32>>;
    static constexpr bool is_int_division =
            std::is_same_v<Operation<Int32, Int32>, DivideIntegralImpl<Int32, Int32>>;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = Operation<NativeResultType, NativeResultType>;

    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;

    static void vector_vector(const typename Traits::ArrayA& a, const typename Traits::ArrayB& b,
                              ArrayC& c, ResultType scale_a [[maybe_unused]],
                              ResultType scale_b [[maybe_unused]]) {
        size_t size = a.size();
        if constexpr (is_plus_minus) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<true>(a[i], b[i], scale_a);
                }
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<false>(a[i], b[i], scale_b);
                }
                return;
            }
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b[i]);
        }
    }

    /// null_map for divide and mod
    static void vector_vector(const typename Traits::ArrayA& a, const typename Traits::ArrayB& b,
                              ArrayC& c, ResultType scale_a [[maybe_unused]],
                              ResultType scale_b [[maybe_unused]], NullMap& null_map) {
        size_t size = a.size();

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b[i], null_map[i]);
        }
    }

    static void vector_constant(const typename Traits::ArrayA& a, B b, ArrayC& c,
                                ResultType scale_a [[maybe_unused]],
                                ResultType scale_b [[maybe_unused]]) {
        size_t size = a.size();
        if constexpr (is_plus_minus) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<true>(a[i], b, scale_a);
                }
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<false>(a[i], b, scale_b);
                }
                return;
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a[i], b, scale_a);
            }
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b);
        }
    }

    static void vector_constant(const typename Traits::ArrayA& a, B b, ArrayC& c,
                                ResultType scale_a [[maybe_unused]],
                                ResultType scale_b [[maybe_unused]], NullMap& null_map) {
        size_t size = a.size();
        if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a[i], b, scale_a, null_map[i]);
            }
            return;
        }

        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b, null_map[i]);
        }
    }

    static void constant_vector(A a, const typename Traits::ArrayB& b, ArrayC& c,
                                ResultType scale_a [[maybe_unused]],
                                ResultType scale_b [[maybe_unused]]) {
        size_t size = b.size();
        if constexpr (is_plus_minus) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<true>(a, b[i], scale_a);
                }
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled<false>(a, b[i], scale_b);
                }
                return;
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a, b[i], scale_a);
            }
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a, b[i]);
        }
    }

    static void constant_vector(A a, const typename Traits::ArrayB& b, ArrayC& c,
                                ResultType scale_a [[maybe_unused]],
                                ResultType scale_b [[maybe_unused]], NullMap& null_map) {
        size_t size = b.size();
        if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a, b[i], scale_a, null_map[i]);
            }
            return;
        }

        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a, b[i], null_map[i]);
        }
    }

    static ResultType constant_constant(A a, B b, ResultType scale_a [[maybe_unused]],
                                        ResultType scale_b [[maybe_unused]]) {
        if constexpr (is_plus_minus) {
            if (scale_a != 1) {
                return apply_scaled<true>(a, b, scale_a);
            } else if (scale_b != 1) {
                return apply_scaled<false>(a, b, scale_b);
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            return apply_scaled_div(a, b, scale_a);
        }
        return apply(a, b);
    }

    static ResultType constant_constant(A a, B b, ResultType scale_a [[maybe_unused]],
                                        ResultType scale_b [[maybe_unused]], UInt8& is_null) {
        if constexpr (is_plus_minus) {
            if (scale_a != 1) {
                return apply_scaled<true>(a, b, scale_a, is_null);
            } else if (scale_b != 1) {
                return apply_scaled<false>(a, b, scale_b, is_null);
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            return apply_scaled_div(a, b, scale_a, is_null);
        }
        return apply(a, b, is_null);
    }

    static ColumnPtr adapt_decimal_constant_constant(A a, B b, UInt32 scale, ResultType scale_a,
                                                     ResultType scale_b) {
        auto column_result = ColumnDecimal<ResultType>::create(1, scale);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(1, 0);
            column_result->get_element(0) =
                    constant_constant(a, b, scale_a, scale_b, null_map->get_element(0));
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            column_result->get_element(0) = constant_constant(a, b, scale_a, scale_b);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_constant(ColumnPtr column_left, B b, UInt32 column_scale,
                                                   ResultType scale_a, ResultType scale_b) {
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_result = ColumnDecimal<ResultType>::create(column_left->size(), column_scale);
        DCHECK(column_left_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            vector_constant(column_left_ptr->get_data(), b, column_result->get_data(), scale_a,
                            scale_b, null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_constant(column_left_ptr->get_data(), b, column_result->get_data(), scale_a,
                            scale_b);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_constant_vector(A a, ColumnPtr column_right, UInt32 column_scale,
                                                   ResultType scale_a, ResultType scale_b) {
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);
        auto column_result = ColumnDecimal<ResultType>::create(column_right->size(), column_scale);
        DCHECK(column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            constant_vector(a, column_right_ptr->get_data(), column_result->get_data(), scale_a,
                            scale_b, null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            constant_vector(a, column_right_ptr->get_data(), column_result->get_data(), scale_a,
                            scale_b);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                                 UInt32 column_scale, ResultType scale_a,
                                                 ResultType scale_b) {
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);

        auto column_result = ColumnDecimal<ResultType>::create(column_left->size(), column_scale);
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                          column_result->get_data(), scale_a, scale_b, null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                          column_result->get_data(), scale_a, scale_b);
            return column_result;
        }
    }

private:
    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b) {
        // Now, Doris only support decimal +-*/ decimal.
        // overflow in consider in operator
        DecimalV2Value l(a);
        DecimalV2Value r(b);
        auto ans = Op::template apply(l, r);
        NativeResultType result;
        memcpy(&result, &ans, sizeof(NativeResultType));
        return result;
    }

    /// null_map for divide and mod
    static NativeResultType apply(NativeResultType a, NativeResultType b, UInt8& is_null) {
        DecimalV2Value l(a);
        DecimalV2Value r(b);
        auto ans = Op::template apply(l, r, is_null);
        NativeResultType result;
        memcpy(&result, &ans, std::min(sizeof(result), sizeof(ans)));
        return result;
    }

    template <bool scale_left>
    static NativeResultType apply_scaled(NativeResultType a, NativeResultType b,
                                         NativeResultType scale) {
        if constexpr (is_plus_minus) {
            NativeResultType res;

            if constexpr (check_overflow) {
                bool overflow = false;
                if constexpr (scale_left) {
                    overflow |= common::mul_overflow(a, scale, a);
                } else {
                    overflow |= common::mul_overflow(b, scale, b);
                }

                if constexpr (can_overflow) {
                    overflow |= Op::template apply<NativeResultType>(a, b, res);
                } else {
                    res = Op::template apply<NativeResultType>(a, b);
                }

                if (overflow) {
                    LOG(FATAL) << "Decimal math overflow";
                }
            } else {
                if constexpr (scale_left) {
                    a *= scale;
                } else {
                    b *= scale;
                }
                res = apply(a, b);
            }

            return res;
        }
    }

    static NativeResultType apply_scaled_div(NativeResultType a, NativeResultType b,
                                             NativeResultType scale, UInt8& is_null) {
        if constexpr (is_division) {
            if constexpr (check_overflow) {
                bool overflow = false;
                if constexpr (!IsDecimalNumber<A>) {
                    overflow |= common::mul_overflow(scale, scale, scale);
                }
                overflow |= common::mul_overflow(a, scale, a);
                if (overflow) {
                    LOG(FATAL) << "Decimal math overflow";
                }
            } else {
                if constexpr (!IsDecimalNumber<A>) {
                    scale *= scale;
                }
                a *= scale;
            }

            return apply(a, b, is_null);
        }
    }
};

/// Used to indicate undefined operation
struct InvalidType;

template <bool V, typename T>
struct Case : std::bool_constant<V> {
    using type = T;
};

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true; InvalidType if none.
template <typename... Ts>
using Switch = typename std::disjunction<Ts..., Case<true, InvalidType>>::type;

template <typename DataType>
constexpr bool IsIntegral = false;
template <>
inline constexpr bool IsIntegral<DataTypeUInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt64> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt64> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt128> = true;

template <typename DataType>
constexpr bool IsFloatingPoint = false;
template <>
inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <>
inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename T0, typename T1>
constexpr bool UseLeftDecimal = false;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal32>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal64>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

template <typename T>
using DataTypeFromFieldType =
        std::conditional_t<std::is_same_v<T, NumberTraits::Error>, InvalidType, DataTypeNumber<T>>;

template <template <typename, typename> class Operation, typename LeftDataType,
          typename RightDataType>
struct BinaryOperationTraits {
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;

private: /// it's not correct for Decimal
    using Op = Operation<T0, T1>;

public:
    static constexpr bool allow_decimal =
            std::is_same_v<Operation<T0, T0>, PlusImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, MinusImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, MultiplyImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, ModuloImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, DivideFloatingImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, DivideIntegralImpl<T0, T0>>;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
            /// Decimal cases
            Case<!allow_decimal &&
                         (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>),
                 InvalidType>,
            Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         UseLeftDecimal<LeftDataType, RightDataType>,
                 LeftDataType>,
            Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>,
                 RightDataType>,
            Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> &&
                         IsIntegral<RightDataType>,
                 LeftDataType>,
            Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         IsIntegral<LeftDataType>,
                 RightDataType>,
            /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
            Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> &&
                         !IsIntegral<RightDataType>,
                 InvalidType>,
            Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         !IsIntegral<LeftDataType>,
                 InvalidType>,
            /// number <op> number -> see corresponding impl
            Case<!IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType>,
                 DataTypeFromFieldType<typename Op::ResultType>>>;
};

template <typename LeftDataType, typename RightDataType,
          template <typename, typename> class Operation, bool is_to_null_type>
struct ConstOrVectorAdapter {
    static constexpr bool result_is_decimal =
            IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;
    static constexpr bool is_division =
            std::is_same_v<Operation<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>> ||
            std::is_same_v<Operation<UInt8, UInt8>, DivideIntegralImpl<UInt8, UInt8>>;
    static constexpr bool is_multiply =
            std::is_same_v<Operation<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;

    using ResultDataType =
            typename BinaryOperationTraits<Operation, LeftDataType, RightDataType>::ResultDataType;
    using ResultType = typename ResultDataType::FieldType;
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;
    using ColumnVectorResult =
            std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>,
                               ColumnVector<ResultType>>;

    using OperationImpl = std::conditional_t<
            IsDataTypeDecimal<ResultDataType>,
            DecimalBinaryOperation<A, B, Operation, ResultType, is_to_null_type>,
            BinaryOperationImpl<A, B, Operation<A, B>, is_to_null_type, ResultType>>;

    static ColumnPtr execute(ColumnPtr column_left, ColumnPtr column_right,
                             const LeftDataType& type_left, const RightDataType& type_right) {
        bool is_const_left = is_column_const(*column_left);
        bool is_const_right = is_column_const(*column_right);

        if (is_const_left && is_const_right) {
            return constant_constant(column_left, column_right, type_left, type_right);
        } else if (is_const_left) {
            return constant_vector(column_left, column_right, type_left, type_right);
        } else if (is_const_right) {
            return vector_constant(column_left, column_right, type_left, type_right);
        } else {
            return vector_vector(column_left, column_right, type_left, type_right);
        }
    }

private:
    static auto get_decimal_infos(const LeftDataType& type_left, const RightDataType& type_right) {
        ResultDataType type = decimal_result_type(type_left, type_right, is_multiply, is_division);
        typename ResultDataType::FieldType scale_a = type.scale_factor_for(type_left, is_multiply);
        typename ResultDataType::FieldType scale_b =
                type.scale_factor_for(type_right, is_multiply || is_division);
        return std::make_tuple(type, scale_a, scale_b);
    }

    static ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right,
                                       const LeftDataType& type_left,
                                       const RightDataType& type_right) {
        auto column_left_ptr = check_and_get_column<ColumnConst>(column_left);
        auto column_right_ptr = check_and_get_column<ColumnConst>(column_right);
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        if constexpr (result_is_decimal) {
            auto [type, scale_a, scale_b] = get_decimal_infos(type_left, type_right);

            column_result = OperationImpl::adapt_decimal_constant_constant(
                    column_left_ptr->template get_value<A>(),
                    column_right_ptr->template get_value<B>(), type.get_scale(), scale_a, scale_b);

        } else {
            column_result = OperationImpl::adapt_normal_constant_constant(
                    column_left_ptr->template get_value<A>(),
                    column_right_ptr->template get_value<B>());
        }

        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    static ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right,
                                     const LeftDataType& type_left,
                                     const RightDataType& type_right) {
        auto column_right_ptr = check_and_get_column<ColumnConst>(column_right);
        DCHECK(column_right_ptr != nullptr);

        if constexpr (result_is_decimal) {
            auto [type, scale_a, scale_b] = get_decimal_infos(type_left, type_right);

            return OperationImpl::adapt_decimal_vector_constant(
                    column_left->get_ptr(), column_right_ptr->template get_value<B>(),
                    type.get_scale(), scale_a, scale_b);
        } else {
            return OperationImpl::adapt_normal_vector_constant(
                    column_left->get_ptr(), column_right_ptr->template get_value<B>());
        }
    }

    static ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right,
                                     const LeftDataType& type_left,
                                     const RightDataType& type_right) {
        auto column_left_ptr = check_and_get_column<ColumnConst>(column_left);
        DCHECK(column_left_ptr != nullptr);

        if constexpr (result_is_decimal) {
            auto [type, scale_a, scale_b] = get_decimal_infos(type_left, type_right);

            return OperationImpl::adapt_decimal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr(),
                    type.get_scale(), scale_a, scale_b);
        } else {
            return OperationImpl::adapt_normal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr());
        }
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                   const LeftDataType& type_left, const RightDataType& type_right) {
        if constexpr (result_is_decimal) {
            auto [type, scale_a, scale_b] = get_decimal_infos(type_left, type_right);

            return OperationImpl::adapt_decimal_vector_vector(column_left->get_ptr(),
                                                              column_right->get_ptr(),
                                                              type.get_scale(), scale_a, scale_b);
        } else {
            return OperationImpl::adapt_normal_vector_vector(column_left->get_ptr(),
                                                             column_right->get_ptr());
        }
    }
};

template <template <typename, typename> class Op, typename Name, bool is_to_null_type>
class FunctionBinaryArithmetic : public IFunction {
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Op<int, int>>()))>;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                                   DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                                   DataTypeDecimal<Decimal128>>(type, std::forward<F>(f));
    }

    template <typename F>
    static bool cast_both_types(const IDataType* left, const IDataType* right, F&& f) {
        return cast_type(left, [&](const auto& left_) {
            return cast_type(right, [&](const auto& right_) { return f(left_, right_); });
        });
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryArithmetic>(); }

    FunctionBinaryArithmetic() = default;
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Op<int, int>::get_variadic_argument_types();
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr type_res;
        bool valid = cast_both_types(
                arguments[0].get(), arguments[1].get(), [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Op, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            constexpr bool is_multiply =
                                    std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                            constexpr bool is_division = false;

                            ResultDataType result_type =
                                    decimal_result_type(left, right, is_multiply, is_division);
                            type_res = std::make_shared<ResultDataType>(result_type.get_precision(),
                                                                        result_type.get_scale());
                        } else if constexpr (IsDataTypeDecimal<LeftDataType>) {
                            type_res = std::make_shared<LeftDataType>(left.get_precision(),
                                                                      left.get_scale());
                        } else if constexpr (IsDataTypeDecimal<RightDataType>) {
                            type_res = std::make_shared<RightDataType>(right.get_precision(),
                                                                       right.get_scale());
                        } else if constexpr (IsDataTypeDecimal<ResultDataType>) {
                            type_res = std::make_shared<ResultDataType>(27, 9);
                        } else {
                            type_res = std::make_shared<ResultDataType>();
                        }
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            LOG(FATAL) << fmt::format("Illegal types {} and {} of arguments of function {}",
                                      arguments[0]->get_name(), arguments[1]->get_name(),
                                      get_name());
        }

        if constexpr (is_to_null_type) {
            return make_nullable(type_res);
        }

        return type_res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto* left_generic = block.get_by_position(arguments[0]).type.get();
        auto* right_generic = block.get_by_position(arguments[1]).type.get();
        if (left_generic->is_nullable()) {
            left_generic =
                    static_cast<const DataTypeNullable*>(left_generic)->get_nested_type().get();
        }
        if (right_generic->is_nullable()) {
            right_generic =
                    static_cast<const DataTypeNullable*>(right_generic)->get_nested_type().get();
        }

        bool valid = cast_both_types(
                left_generic, right_generic, [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Op, LeftDataType,
                                                           RightDataType>::ResultDataType;

                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        auto column_result = ConstOrVectorAdapter<LeftDataType, RightDataType, Op,
                                                                  is_to_null_type>::
                                execute(block.get_by_position(arguments[0]).column,
                                        block.get_by_position(arguments[1]).column, left, right);
                        block.replace_by_position(result, std::move(column_result));
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError(
                    fmt::format("{}'s arguments do not match the expected data types", get_name()));
        }

        return Status::OK();
    }
};

} // namespace doris::vectorized
