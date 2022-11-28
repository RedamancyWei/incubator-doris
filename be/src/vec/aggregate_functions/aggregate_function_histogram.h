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
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column_array.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

#include <sstream>

#include "gen_cpp/Exprs_types.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.h"

#include "runtime/primitive_type.h"

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column_array.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column_array.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

#include "vec/data_types/data_type_factory.hpp"
#include "util/slice.h"

namespace doris::vectorized {

template<typename T>
struct Bucket {
public:
    Bucket() = default;

    Bucket(T lower, T upper, size_t count, size_t upper_repeats)
            : lower(lower), upper(upper), count(count), upper_repeats(upper_repeats), count_in_bucket(1) {}

    T lower;
    T upper;
    // Up to this bucket, the total value
    int64_t count;
    // the number of values that on the upper boundary
    int64_t upper_repeats;
    // total value count in this bucket
    int64_t count_in_bucket;
};

template<typename T>
struct AggregateFunctionHistogramData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    PaddedPODArray<ElementType> data;

    void add(const IColumn &column, size_t row_num) {
        const auto &vec = assert_cast<const ColVecType &>(column).get_data();
        data.push_back(vec[row_num]);
    }

    void write(BufferWritable &buf) const {
        write_var_uint(data.size(), buf);
        buf.write(data.raw_data(), data.size() * sizeof(ElementType));
    }

    void read(BufferReadable &buf) {
        UInt64 rows = 0;
        read_var_uint(rows, buf);
        data.resize(rows);
        buf.read(reinterpret_cast<char *>(data.data()), rows * sizeof(ElementType));
    }

    void merge(const AggregateFunctionHistogramData &rhs) {
        data.insert(rhs.data.begin(), rhs.data.end());
    }

    void insert_result_into(IColumn &to) const {
        auto &vec = assert_cast<ColVecType &>(to).get_data();
        size_t old_size = vec.size();
        vec.resize(old_size + data.size());
        memcpy(vec.data() + old_size, data.data(), data.size() * sizeof(ElementType));
    }

    std::vector<ElementType> get() const {
        std::vector<ElementType> vec_data;

        for (size_t i = 0; i < data.size(); ++i) {
            [[maybe_unused]] ElementType d = data[i];
            vec_data.push_back(d);
        }

        return vec_data;
    }

    void reset() { data.clear(); }

    void set_parameters(int input_bucket_num, double input_sample_ratio = 0) {
        bucket_num = input_bucket_num;
        sample_ratio = input_sample_ratio;
    }

    int bucket_num = 0;
    double sample_ratio = 0;
};

template<>
struct AggregateFunctionHistogramData<StringRef> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;

    AggregateFunctionHistogramData<ElementType>() { data = ColVecType::create(); }

    void add(const IColumn &column, size_t row_num) { data->insert_from(column, row_num); }

    void write(BufferWritable &buf) const {
        auto &col = assert_cast<ColVecType &>(*data);

        write_var_uint(col.size(), buf);
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        write_var_uint(col.get_chars().size(), buf);
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
    }

    void read(BufferReadable &buf) {
        auto &col = assert_cast<ColVecType &>(*data);
        UInt64 offs_size = 0;
        read_var_uint(offs_size, buf);
        col.get_offsets().resize(offs_size);
        buf.read(reinterpret_cast<char *>(col.get_offsets().data()),
                 offs_size * sizeof(IColumn::Offset));

        UInt64 chars_size = 0;
        read_var_uint(chars_size, buf);
        col.get_chars().resize(chars_size);
        buf.read(reinterpret_cast<char *>(col.get_chars().data()), chars_size);
    }

    void merge(const AggregateFunctionHistogramData &rhs) {
        data->insert_range_from(*rhs.data, 0, rhs.data->size());
    }

    void insert_result_into(IColumn &to) const {
        auto &to_str = assert_cast<ColVecType &>(to);
        to_str.insert_range_from(*data, 0, data->size());
    }

    std::vector<ElementType> get() const {
        std::vector<ElementType> vec_data;
        auto* res_column = reinterpret_cast<ColVecType*>(data.get());

        for (int i = 0; i < res_column->size(); ++i) {
            [[maybe_unused]] ElementType c = res_column->get_data_at(i);
            vec_data.push_back(c);
            LOG(WARNING) << fmt::format("ElementType==========> {}", c.to_string());
        }

        return vec_data;
    }

    void reset() { data->clear(); }

    void set_parameters(int input_bucket_num, double input_sample_ratio = 0) {
        bucket_num = input_bucket_num;
        sample_ratio = input_sample_ratio;
    }

    int bucket_num = 0;
    double sample_ratio = 0;
};

template<typename Data, typename T>
class AggregateFunctionHistogram final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionHistogram<Data, T>> {
public:
    AggregateFunctionHistogram(const DataTypes &argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionHistogram<Data, T>>(argument_types_,
                                                                                      {}),
              _argument_type(argument_types_[0]) {}

    using ColVecType = std::conditional_t<
            std::is_same_v<T, StringRef>, ColumnString,
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>>;

    std::string get_name() const override {
        return "histogram";
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeString>();
        // return std::make_shared<DataTypeArray>(make_nullable(_argument_type));
    }

    void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num,
             Arena *arena) const override {
        // assert(!columns[0]->is_null_at(row_num));
        if (columns[0]->is_null_at(row_num)) {
            return;
        }
        this->data(place).add(*columns[0], row_num);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena *arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable &buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable &buf,
                     Arena *) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn &to) const override {


        // To json
        std::string bucket_json = "哈哈";
        const std::vector<T> res_column = this->data(place).get();
        std::sort(res_column.begin(), res_column.end());

        if (!res_column.empty()) {
            for(int i=0; i < res_column.size(); i++) {
                [[maybe_unused]] T t0 = res_column[i];
                [[maybe_unused]] T t1 = res_column[i];
                if (std::is_same_v<T, StringRef>) {
//                    auto t0_ = assert_cast<StringRef>(t0);
//                    auto t1_ = assert_cast<StringRef>(t1);
//                    [[maybe_unused]] std::string s0 = t0_->to_string();
//                    [[maybe_unused]] std::string s1 = t1_->to_string();
                }
                // const StringRef& s = assert_cast<const StringRef&>();
            }
        }

        assert_cast<ColumnString &>(to).insert_data(bucket_json.c_str(), bucket_json.length());
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized