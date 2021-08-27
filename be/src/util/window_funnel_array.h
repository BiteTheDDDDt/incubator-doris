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

//Tcompare: 
//Tid: tinyint,smallint,largeint,int,bigint,char,varchar

template <class Tcompare, class Tid>
class WindowFunnelArray {
public:
    void add(std::pair<Tcompare, Tid> element) { array.emplace_back(element); }
    void merge(WindowFunnelArray other) { array.emplace_back(other.begin(), other.end()); }
    uint32_t serialized_size() { return (sizeof(Tcompare) + sizeof(Tid)) * array.size()+sizeof(uint32_t); }

    void serialize(uint8_t* writer) {
        memcpy(writer,sizeof(uint32_t),size(sizeof(uint32_t)));
        uint32_t first_size = sizeof(Tcompare);
        uint32_t second_size = sizeof(Tid);
        for (auto element : array) {
            memcpy(writer, &element.first, sizeof(first_size));
            writer += first_size;
            memcpy(writer, &element.second, sizeof(second_size));
            writer += second_size;
        }
    }
    void unserialize(const uint8_t* type_reader)
    {
        uint32_t size;
        memcpy(*size,)
    }
    int get_max_chain_length();

private:
    std::vector<std::pair<Tcompare, Tid>> array;
};