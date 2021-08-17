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

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <iostream>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "olap/comparison_predicate.h"
#include "olap/fs/block_manager.h"
#include "olap/fs/fs_util.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_common.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "test_util/test_util.h"
#include "util/file_utils.h"

#include <benchmark/benchmark.h>


namespace doris {
namespace segment_v2 {

std::string us_to_ms(int64_t ms) {
    return std::to_string(ms / 1000000) + "." + std::to_string(ms % 1000000) + "ms";
}

namespace segment_layer {

const std::string kSegmentDir = "./ut_dir/benchmark_test";

using std::string;
using std::shared_ptr;

using std::vector;

using ValueGenerator = std::function<void(size_t rid, int cid, int block_id, RowCursorCell& cell)>;

// 0,  1,  2,  3
// 10, 11, 12, 13
// 20, 21, 22, 23
static void DefaultIntGenerator(size_t rid, int cid, int block_id, RowCursorCell& cell) {
    cell.set_not_null();
    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
}

static bool column_contains_index(ColumnMetaPB column_meta, ColumnIndexTypePB type) {
    for (int i = 0; i < column_meta.indexes_size(); ++i) {
        if (column_meta.indexes(i).type() == type) {
            return true;
        }
    }
    return false;
}

void SetUp() {
    if (FileUtils::check_exist(kSegmentDir)) {
        ASSERT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
    }
    ASSERT_TRUE(FileUtils::create_dir(kSegmentDir).ok());
}

void TearDown() {
    if (FileUtils::check_exist(kSegmentDir)) {
        ASSERT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
    }
}

TabletSchema create_schema(const std::vector<TabletColumn>& columns,
                           int num_short_key_columns = -1) {
    TabletSchema res;
    int num_key_columns = 0;
    for (auto& col : columns) {
        if (col.is_key()) {
            num_key_columns++;
        }
        res._cols.push_back(col);
    }
    res._num_columns = columns.size();
    res._num_key_columns = num_key_columns;
    res._num_short_key_columns =
            num_short_key_columns != -1 ? num_short_key_columns : num_key_columns;
    res.init_field_index_for_test();
    return res;
}

void build_segment(SegmentWriterOptions opts, const TabletSchema& build_schema,
                   const TabletSchema& query_schema, size_t nrows, const ValueGenerator& generator,
                   shared_ptr<Segment>* res) {
    static int seg_id = 0;
    // must use unique filename for each segment, otherwise page cache kicks in and produces
    // the wrong answer (it use (filename,offset) as cache key)
    std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions block_opts({filename});
    Status st = fs::fs_util::block_manager()->create_block(block_opts, &wblock);
    ASSERT_TRUE(st.ok());
    SegmentWriter writer(wblock.get(), 0, &build_schema, opts);
    st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(build_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    for (size_t rid = 0; rid < nrows; ++rid) {
        for (int cid = 0; cid < build_schema.num_columns(); ++cid) {
            int row_block_id = rid / opts.num_rows_per_block;
            RowCursorCell cell = row.cell(cid);
            generator(rid, cid, row_block_id, cell);
        }
        ASSERT_TRUE(writer.append_row(row).ok());
    }

    uint64_t file_size, index_size;
    st = writer.finalize(&file_size, &index_size);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(wblock->close().ok());

    st = Segment::open(filename, 0, &query_schema, res);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(nrows, (*res)->num_rows());
}

void benchmark_read_data_pure_int_x4(const int& data_row_num) {
    std::cout << "start test: benchmark_read_data_pure_int_x4 x" << data_row_num << std::endl;

    TabletSchema tablet_schema = create_schema(
            {create_int_key(1), create_int_key(2), create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;

    shared_ptr<Segment> segment;
    build_segment(opts, tablet_schema, tablet_schema, data_row_num, DefaultIntGenerator, &segment);

    Schema schema(tablet_schema);
    OlapReaderStatistics stats;

    StorageReadOptions read_opts;
    read_opts.stats = &stats;
    std::unique_ptr<RowwiseIterator> iter;
    segment->new_iterator(schema, read_opts, nullptr, &iter);

    RowBlockV2 block(schema, 1024);

    // scan all rows
    {
        StorageReadOptions read_opts;
        read_opts.stats = &stats;
        std::unique_ptr<RowwiseIterator> iter;
        segment->new_iterator(schema, read_opts, nullptr, &iter);

        RowBlockV2 block(schema, 1024);

        int64_t scan_all_rows_ns = 0;
        int left = data_row_num;
        {
            SCOPED_RAW_TIMER(&scan_all_rows_ns);
            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                iter->next_batch(&block);
                left -= rows_read;
                rowid += rows_read;
            }
        }
        std::cout << "scan all rows use: " << us_to_ms(scan_all_rows_ns) << std::endl;
    }
}

void benchmark_write_data_pure_int_x4(const int& data_row_num) {
    std::cout << "start test: benchmark_write_data_pure_int_x4 x" << data_row_num << std::endl;

    TabletSchema tablet_schema = create_schema(
            {create_int_key(1), create_int_key(2), create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;

    static int seg_id = 0;

    std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions block_opts({filename});
    Status st = fs::fs_util::block_manager()->create_block(block_opts, &wblock);
    ASSERT_TRUE(st.ok());
    SegmentWriter writer(wblock.get(), 0, &tablet_schema, opts);
    st = writer.init(1024);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    {
        int64_t append_all_rows_ns = 0;
        {
            for (size_t rid = 0; rid < data_row_num; ++rid) {
                for (int cid = 0; cid < tablet_schema.num_columns(); ++cid) {
                    int row_block_id = rid / opts.num_rows_per_block;
                    RowCursorCell cell = row.cell(cid);
                    DefaultIntGenerator(rid, cid, row_block_id, cell);
                }
                {
                    SCOPED_RAW_TIMER(&append_all_rows_ns);
                    writer.append_row(row);
                }
            }
        }
        std::cout << "append all rows use: " << us_to_ms(append_all_rows_ns) << std::endl;
        int64_t finalize_all_rows_ns = 0;
        {
            SCOPED_RAW_TIMER(&finalize_all_rows_ns);
            uint64_t file_size, index_size;
            writer.finalize(&file_size, &index_size);
            wblock->close();
        }
        std::cout << "finalize all rows use: " << us_to_ms(finalize_all_rows_ns) << std::endl;
    }
}

void start_test(std::function<void(const int&)> func, int arg) {
    SetUp();
    func(arg);
    TearDown();
}

void run_all_test() {
    std::cout << "------------------------------" << std::endl
              << "|start run segment layer test|" << std::endl
              << "------------------------------" << std::endl;

    start_test(benchmark_read_data_pure_int_x4, 100000);
    start_test(benchmark_write_data_pure_int_x4, 100000);
}
} // namespace segment_layer

namespace column_layer {

static const std::string TEST_DIR = "./ut_dir/column_reader_writer_test";

void SetUp() {
    if (FileUtils::check_exist(TEST_DIR)) {
        ASSERT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
    }
    ASSERT_TRUE(FileUtils::create_dir(TEST_DIR).ok());
}

void TearDown() {
    if (FileUtils::check_exist(TEST_DIR)) {
        ASSERT_TRUE(FileUtils::remove_all(TEST_DIR).ok());
    }
}

} // namespace column_layer

namespace bitshuffle_page_layer {

using doris::segment_v2::PageBuilderOptions;

void benchmark_write_data_int(size_t data_row_num) {
    auto Type = OLAP_FIELD_TYPE_INT;
    using PageDecoderType = BitShufflePageDecoder<OLAP_FIELD_TYPE_INT>;
    using PageBuilderType = BitshufflePageBuilder<OLAP_FIELD_TYPE_INT>;
    using CppType = int32_t;

    {
        std::unique_ptr<int32_t[]> ints(new int32_t[data_row_num]);
        for (int i = 0; i < data_row_num; i++) {
            ints.get()[i] = i;
        }
        auto src = ints.get();
        PageBuilderOptions options;
        options.data_page_size = 1024 * 1024;
        PageBuilderType page_builder(options);

        int64_t add_all_data_ns = 0;
        int64_t finish_all_data_ns = 0;
        OwnedSlice s;
        {
            {
                SCOPED_RAW_TIMER(&add_all_data_ns);
                page_builder.add(reinterpret_cast<const uint8_t*>(src), &data_row_num);
            }
            SCOPED_RAW_TIMER(&finish_all_data_ns);
            s = page_builder.finish();
        }
        std::cout << "once add all data use: " << us_to_ms(add_all_data_ns) << std::endl;
        std::cout << "once finish all data use: " << us_to_ms(finish_all_data_ns) << std::endl;
    }

    {
        std::unique_ptr<int32_t[]> ints(new int32_t[data_row_num]);
        for (int i = 0; i < data_row_num; i++) {
            ints.get()[i] = i;
        }
        auto src = ints.get();
        PageBuilderOptions options;
        options.data_page_size = 1024 * 1024;
        PageBuilderType page_builder(options);

        int64_t add_all_data_ns = 0;
        int64_t finish_all_data_ns = 0;
        OwnedSlice s;
        {
            for (int i = 0; i < data_row_num; i++) {
                size_t size = 1;
                SCOPED_RAW_TIMER(&add_all_data_ns);
                page_builder.add(reinterpret_cast<const uint8_t*>(&src[i]), &size);
            }
            SCOPED_RAW_TIMER(&finish_all_data_ns);
            s = page_builder.finish();
        }
        std::cout << "one by one add all data use: " << us_to_ms(add_all_data_ns) << std::endl;
        std::cout << "one by one finish all data use: " << us_to_ms(finish_all_data_ns)
                  << std::endl;
    }

    /*
    //check first value and last value
    CppType first_value;
    page_builder.get_first_value(&first_value);
    ASSERT_EQ(src[0], first_value);
    CppType last_value;
    page_builder.get_last_value(&last_value);
    ASSERT_EQ(src[data_row_num - 1], last_value);

    segment_v2::PageDecoderOptions decoder_options;
    PageDecoderType page_decoder(s.slice(), decoder_options);
    Status status = page_decoder.init();
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, page_decoder.current_index());

    auto tracker = std::make_shared<MemTracker>();
    MemPool pool(tracker.get());

    std::unique_ptr<ColumnVectorBatch> cvb;
    ColumnVectorBatch::create(data_row_num, false, get_scalar_type_info(Type), nullptr, &cvb);
    ColumnBlock block(cvb.get(), &pool);
    ColumnBlockView column_block_view(&block);

    status = page_decoder.next_batch(&data_row_num, &column_block_view);
    ASSERT_TRUE(status.ok());

    CppType* values = reinterpret_cast<CppType*>(block.data());
    CppType* decoded = (CppType*)values;
    for (uint i = 0; i < data_row_num; i++) {
        if (src[i] != decoded[i]) {
            FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << decoded[i];
        }
    }
*/
}

void start_test(std::function<void(const int&)> func, int arg) {
    func(arg);
}

void run_all_test() {
    std::cout << "--------------------------------------" << std::endl
              << "|start run bitshuffle page layer test|" << std::endl
              << "--------------------------------------" << std::endl;

    start_test(benchmark_write_data_int, 100000); // should less than 1024*1024
}

} // namespace bitshuffle_page_layer

} // namespace segment_v2
} // namespace doris

static void BM_mod(benchmark::State &state)
{
  for (auto _ : state)
  {
    int p = 0;
    int q=0;
    int res=0;
    while (p < 100000)
    {
      p++;
      q++;
      q%=1024;
      res+=q==0;
    }
  }
}
// Register the function as a benchmark
BENCHMARK(BM_mod)->Iterations(100);

// Define another benchmark
static void BM_plus(benchmark::State &state)
{
  std::set<int> s;
  for (auto _ : state)
  {
    int p = 0;
    int q=0;
    int res=0;
    while (p < 100000)
    {
      p++;
      q++;
      if(q==1024)q=0;
      res+=q==0;
    }
  }
}
BENCHMARK(BM_plus)->Iterations(100);

BENCHMARK_MAIN();
