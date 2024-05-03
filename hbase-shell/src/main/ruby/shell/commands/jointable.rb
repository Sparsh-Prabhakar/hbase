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
        # params1["COLUMNS"] = params["ON"]
        params1["FILTER"] = params["FILTER1"]
        params2["FILTER"] = params["FILTER2"]

        # puts params['COLUMNS'].class

        # puts params1.keys.length
        # puts params2.inspect

        limit = params['LIMIT'] || -1
        scan1 = table1._hash_to_scan(params1)
        scan2 = table2._hash_to_scan(params2)

        @start_time = Time.now

        res1 = table1._scan_internal_join(params1, scan1)
        res2 = table2._scan_internal_join(params2, scan2)


        if res1.length == 0 || res2.length == 0
          puts "No matching rows in two tables"
        else
          res1[params["ON"]] ||= {}
          res2[params["ON"]] ||= {}
          join_type = vote_for_joins(table1, table2,res1[params["ON"]], res2[params["ON"]], params1,params2)
          
          if join_type == 'hash'
            join_output, all_columns =  hash_join(table1, table2, res1[params["ON"]], res2[params["ON"]], limit)
          else
            join_output, all_columns = nested_loop_join(table1, table2, res1 ,res2,limit, params['ON'])
          end

          all_columns = all_columns.uniq
          
          if params['PROJECT']
            # puts params['PROJECT']
            all_columns = all_columns & params['PROJECT']
            # puts all_columns
          end

          if all_columns.length == 0
            puts 'No columns present in output of query from', params['PROJECT']
          else
            all_columns.unshift('ROW')
            format_rows(join_output, all_columns, params)
          end
          
        end

        @end_time = Time.now

      end

      def table_size_ratio(table1, table2, params1 = {}, params2 = {})
        count1 = count(table1, params1)
        count2 = count(table2, params2)
        count1.to_f / count2
      end

      def check_join_column_type(table1, table2, res1, res2, params1 = {}, params2 = {})


        dtype = 'int'
        res1.each do |key, value|
          if not (!!Integer(value) rescue false)
            return dtype
          end
        end

        res2.each do |key, value|
          if not (!!Integer(value) rescue false)
            return dtype
          end
        end

        return 'string'
      end

      def get_data_selectivty(table1,table2, res1, res2, params1 = {},params2 = {})
  
        unique_table1 = res1.keys.uniq.length
        unique_table2 = res2.keys.uniq.length

        total1 = res1.keys.length
        total2 = res2.keys.length

        return [total1.to_f / unique_table1 , total2.to_f / unique_table2]
      end

      def vote_for_joins(table1, table2, res1={}, res2={}, params1={},params2={})
        hash_join = 0
        nested_loop_join = 0

        if table_size_ratio(table1 ,table2 ,params1,params2).between?(0.5,2)
          hash_join+=1
        else
          nested_loop_join +=1
        end

        if check_join_column_type(table1 ,table2, res1, res2, params1, params2) == 'string'
          nested_loop_join +=1
        else
          hash_join += 1
        end

        selectivity1, selectivity2 = get_data_selectivty(table1, table2, res1, res2, params1, params2)

        if selectivity1 > 2 and selectivity2 > 2
          nested_loop_join +=1
        else
          hash_join += 1
        end

        puts "Hash join votes: ", hash_join
        puts "Nested loop join votes: ", nested_loop_join
        
        return "hash" ? hash_join > nested_loop_join : 'nested'
      end

      def nested_loop_join(table1, table2,res1,res2,limit, on_condition)
        join_output = {}
        all_columns = []

        join_key = 1

        break_loop = false

        join_indexes = []

        for col1 in res1.keys
          for col2 in res2.keys
            if col1 == col2
              for row1 in res1[col1].keys
                for row2 in res2[col2].keys 
                  if res1[col1][row1] == res2[col2][row2]
                    
                    join_indexes.push([row1, row2])
                  end
                end
              end
            end
          end
        end

        all_columns.concat(res1.keys | res2.keys)
        
        for index_list in join_indexes
          join_output[join_key] ||= {}

          for key in res1.keys
            join_output[join_key][key] = res1[key][index_list[0]]
          end

          for key in res2.keys
            join_output[join_key][key] = res2[key][index_list[1]]
          end
          
          if join_key == limit
            break
          else
            join_key += 1
          end
        end
        
        [join_output,all_columns]
      end


      def hash_join(table1, table2, res1, res2, limit)
        hash_table1 = {}

        join_output = {}
        all_columns = []

        res1.each do |key, value|
          hash_value = murmur_hash3(value)
          hash_table1[hash_value] ||= []
          hash_table1[hash_value] << key
        end
  
        # join_output = {}
        join_key = 1

        res2.each do |key2, value|
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

      def murmur_hash3(key, seed=0)
        # Constants for 32-bit MurmurHash3
        c1 = 0xcc9e2d51
        c2 = 0x1b873593
        r1 = 15
        r2 = 13
        m = 5
        n = 0xe6546b64
      
        # Helper function to perform a single mix
        mix = lambda do |h, k|
          k *= c1
          k = (k << r1) | (k >> (32 - r1))
          k *= c2
      
          h ^= k
          h = ((h << r2) | (h >> (32 - r2))) * m + n
      
          h
        end
      
        # Hash initialization
        length = key.length
        h = seed
        chunk_size = 4
        remainder = length % chunk_size
        ending = length - remainder
      
        # Mix 4-byte chunks
        (0...ending).step(chunk_size) do |i|
          chunk = key[i, chunk_size]
          chunk_int = chunk.unpack("V").first
          h = mix.call(h, chunk_int)
        end
      
        # Handle the remaining bytes
        if remainder > 0
          chunk = key[ending..-1] + "\x00" * (4 - remainder) # Pad remaining bytes with zeroes
          chunk_int = chunk.unpack("V").first
          h ^= chunk_int
          h *= c1
        end
      
        # Finalize hash
        h ^= length
        h ^= (h >> 16)
        h *= 0x85ebca6b
        h ^= (h >> 13)
        h *= 0xc2b2ae35
        h ^= (h >> 16)
      
        h & 0xFFFFFFFF
      end

      def order_join_output(join_output, order_by_column, reversed)
        final_list = {}
        key_value_pairs = []
        for key in join_output.keys
          key_value_pairs.push([key, join_output[key][order_by_column]])
        end

        sorted_list = key_value_pairs.sort_by {|list| list[1]}

        if reversed == 'desc'
          sorted_list = sorted_list.reverse
        end

        count = 0

        for list in sorted_list
          final_list[count] = join_output[list[0]]
          count = count + 1
        end

        return final_list
      end

      def format_rows(join_output, all_columns, params={})
        error = 0

        if params['ORDER BY']
          if all_columns.include?(params['ORDER BY'])
            if params['REVERSE']
              sorted_list = order_join_output(join_output, params['ORDER BY'], params['REVERSE'])
              join_output = sorted_list
            else
              puts "Mention the REVERSE value"
              error = 1
            end
          else
            puts "Column " + params['ORDER BY'] + " not present."
            error = 1
          end
        end
        
        if error == 0

          if params['COLUMNS']
            params['COLUMNS'].push('ROW')
            formatter.header(params['COLUMNS'])
          else
            formatter.header(all_columns)
          end

          join_output.each do |join_key,columns_hash|
            row_data = []
            all_columns.each do |column_name|
              if params['COLUMNS']
                if params['COLUMNS'].include?(column_name)
                  if column_name == 'ROW'
                    row_data << join_key
                  else
                    row_data << columns_hash[column_name]
                  end
                end
              else
                if column_name == 'ROW'
                  row_data << join_key
                else
                  row_data << columns_hash[column_name]
                end
              end
            end
            formatter.row(row_data)
            # puts "Key: #{key}, Value: #{value}"
          end
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
# ::Hbase::Table.add_shell_co mmand('jointable')


# jointable 'class', 'student', {'ON' => 'data:name', 'FILTER1' => "SingleColumnValueFilter('data', 'course_taken', =, 'binary:Deep Learning')"}