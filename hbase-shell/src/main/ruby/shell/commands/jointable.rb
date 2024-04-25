#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
include Java


module Shell
  module Commands
    class Jointable < Command
      def help
        <<-EOF
Count the number of rows in a table.  Return value is the number of rows.
This operation may take a LONG time (Run '$HADOOP_HOME/bin/hadoop jar
hbase.jar rowcount' to run a counting mapreduce job). Current count is shown
every 1000 rows by default. Count interval may be optionally specified. Scan
caching is enabled on count scans by default. Default cache size is 10 rows.
If your rows are small in size, you may want to increase this
parameter. Examples:

 hbase> count 'ns1:t1'
 hbase> count 't1'
 hbase> count 't1', INTERVAL => 100000
 hbase> count 't1', CACHE => 1000
 hbase> count 't1', INTERVAL => 10, CACHE => 1000
 hbase> count 't1', FILTER => "
    (QualifierFilter (>=, 'binary:xyz')) AND (TimestampsFilter ( 123, 456))"
 hbase> count 't1', COLUMNS => ['c1', 'c2'], STARTROW => 'abc', STOPROW => 'xyz'

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding commands would be:

 hbase> t.count
 hbase> t.count INTERVAL => 100000
 hbase> t.count CACHE => 1000
 hbase> t.count INTERVAL => 10, CACHE => 1000
 hbase> t.count FILTER => "
    (QualifierFilter (>=, 'binary:xyz')) AND (TimestampsFilter ( 123, 456))"
 hbase> t.count COLUMNS => ['c1', 'c2'], STARTROW => 'abc', STOPROW => 'xyz'

By default, this operation does not cause any new blocks to be read into
the RegionServer block cache. This is typically the desired action; however,
if you want to force all blocks for a table to be loaded into the block cache
on-demand, you can pass the 'CACHE_BLOCKS' option with a value of 'true'. A value
of 'false' is the default and will result in no blocks being cached. This
command can be used in conjunction with all other options.

hbase> count 'ns1:t1', CACHE_BLOCKS => true
hbase> count 'ns1:t1', CACHE_BLOCKS => 'true'
hbase> count 'ns1:t1', INTERVAL => 100000, CACHE_BLOCKS => false
EOF
      end

      def command(table, table2, params = {})
        print table
        print " Count Rows = "
        count(table(table), params)

        print table2
        print " Count Rows = "
        count(table(table2), params)

        join_tables(table(table),table(table2), params)

      end

      def join_tables(table1, table2, params={})
        # puts params
        params1 = {}
        params2 = {}
        params1["COLUMNS"] = params["ON"]
        params2["COLUMNS"] = params["ON"]
        params1["FILTER"] = params["FILTER1"]
        params2["FILTER"] = params["FILTER2"]

        puts params.inspect

        limit = params['LIMIT'] || -1

        puts limit

        # params1["LIMIT"] = params["LIMIT"]
        # params2["LIMIT"] = params["LIMIT"]
        # params1 = { "COLUMNS" => params["ON"]}
        scan1 = table1._hash_to_scan(params1)
        scan2 = table2._hash_to_scan(params2)
        puts "Scans created"

        @start_time = Time.now

        res1 = table1._scan_internal_join(params1, scan1)
        res2 = table2._scan_internal_join(params1, scan2)

        join_output, all_columns = nested_loop_join(table1, table2, res1,res2,limit)

        all_columns = all_columns.uniq

        all_columns.unshift('ROW')

        format_rows(join_output, all_columns)

        @end_time = Time.now

      end

      def nested_loop_join(table1, table2,res1,res2,limit)
        join_output = {}
        all_columns = []

        join_key = 1

        break_loop = false

        res1.each do |key1,value1|
          res2.each do |key2,value2|
            if value1 == value2

              row1 = table1._get_internal_join(key1,{})
              row2 = table2._get_internal_join(key2,{})

              join_output[join_key] ||= {}

              row1.each do |column,value|
                join_output[join_key][column] = value
              end

              row2.each do |column,value|
                join_output[join_key][column] = value
              end

              all_columns.concat(row1.keys | row2.keys)
              if join_key == limit
                break_loop = true
                break
              else
                join_key += 1
              end
            end
          end
          if break_loop == true
            break
          end
        end
        
        [join_output,all_columns]
      end


      def hash_join(table1,table2,limit)
        hash_table1 = {}
        table1.each do |key, value|
          hash_value = murmur_hash3(value)
          hash_table1[hash_value] ||= []
          hash_table1[hash_value] << key
        end
  
        join_output = {}
        join_key = 1

        table2.each do |key2, value|
          hash_value = murmur_hash3(value)
          if hash_table1[hash_value]
            hash_table1[hash_value].each do |table1_key|

              row1 = table1._get_internal_join(table1_key,{})
              row2 = table2._get_internal_join(key2,{})

              join_output[join_key] ||= {}

              row1.each do |column,value|
                join_output[join_key][column] = value
              end

              row2.each do |column,value|
                join_output[join_key][column] = value
              end

              all_columns.concat(row1.keys | row2.keys)
              if join_key == limit
                break
              else
                join_key += 1
              end
            end
          end
        end

        [join_output,all_columns]
      end

      def murmur_hash3(key, seed = 0)
        c1 = 0xcc9e2d51
        c2 = 0x1b873593
        r1 = 15
        r2 = 13
        m = 5
        n = 0xe6546b64
      
        length = key.bytesize
        hash_value = seed
      
        key.bytes.each_slice(4) do |slice|
          k = slice.pack('C*').unpack1('L<')
      
          k *= c1
          k &= 0xffffffff
          k = (k << r1) | (k >> (32 - r1))
          k &= 0xffffffff
          k *= c2
          k &= 0xffffffff
      
          hash_value ^= k
          hash_value = ((hash_value << r2) | (hash_value >> (32 - r2))) * m + n
          hash_value &= 0xffffffff
        end
      
        tail = key.byteslice(length & ~3, length & 3)
        unless tail.empty?
          if tail.bytesize >= 3
            hash_value ^= tail[2].ord << 16
          end
          if tail.bytesize >= 2
            hash_value ^= tail[1].ord << 8
          end
          if tail.bytesize >= 1
            hash_value ^= tail[0].ord
          end
          hash_value *= c1
          hash_value &= 0xffffffff
          hash_value ^= hash_value >> 16
          hash_value *= c2
          hash_value &= 0xffffffff
        end
      
        hash_value ^= length
        hash_value ^= hash_value >> 16
        hash_value *= 0x85ebca6b
        hash_value &= 0xffffffff
        hash_value ^= hash_value >> 13
        hash_value *= 0xc2b2ae35
        hash_value &= 0xffffffff
        hash_value ^= hash_value >> 16
      
        hash_value
      end

      def format_rows(join_output, all_columns)
        
        formatter.header(all_columns)

        join_output.each do |join_key,columns_hash|
          row_data = []
          all_columns.each do |column_name|
            if column_name == 'ROW'
              row_data << join_key
            else
              row_data << columns_hash[column_name]
            end
          end
          formatter.row(row_data)
          # puts "Key: #{key}, Value: #{value}"
        end

      end

      def scan(table, args = {})
        formatter.header(['ROW', 'COLUMN+CELL'])

        scan = table._hash_to_scan(args)
        # actually do the scanning
        @start_time = Time.now
        count, is_stale = table._scan_internal(args, scan) do |row, cells|
          formatter.row([row, cells])
        end
        @end_time = Time.now

        formatter.footer(count, is_stale)
        # if scan metrics were enabled, print them after the results
        if !scan.nil? && scan.isScanMetricsEnabled
          formatter.scan_metrics(scan.getScanMetrics, args['METRICS'])
        end
      end

      def count(table, params = {})
        # If the second parameter is an integer, then it is the old command syntax
        params = { 'INTERVAL' => params } if params.is_a?(Integer)

        # Try to be nice and convert a string to a bool
        if params.include?('CACHE_BLOCKS') and params['CACHE_BLOCKS'].is_a?(String)
          if params['CACHE_BLOCKS'].downcase == 'true'
            params['CACHE_BLOCKS'] = true
          elsif params['CACHE_BLOCKS'].downcase == 'false'
            params['CACHE_BLOCKS'] = false
          else
            raise(ArgumentError, "Expected CACHE_BLOCKS value to be a boolean or the string 'true' or 'false'")
          end
        end

        # Merge params with defaults
        params = {
          'INTERVAL' => 1000,
          'CACHE' => 10,
          'CACHE_BLOCKS' => false
        }.merge(params)

        scan = table._hash_to_scan(params)
        # Call the counter method
        @start_time = Time.now
        formatter.header
        count = table._count_internal(params['INTERVAL'].to_i, scan, params['CACHE_BLOCKS']) do |cnt, row|
          formatter.row(["Current count: #{cnt}, row: #{row}"])
        end
        formatter.footer(count)
        count
      end
    end
  end
end

# Add the method table.count that calls count.count
# ::Hbase::Table.add_shell_command('jointable')
