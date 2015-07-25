require 'monetdb'
require 'tempfile'

module MonetDB
  class Connection

    def bulk_load table_name, file_path, delims, null_character

      output = nil

      check_connectivity!
      raise ConnectionError, "Not connected to server" unless connected?

      delim_str = delims.map { |d| "'#{d}'" }.join(',')
      statement = "COPY INTO #{table_name} FROM STDIN"
      statement << " USING DELIMITERS #{delim_str}" if delims && delims.any?
      statement << " NULL AS '#{null_character}'" if null_character

      begin
        credentials = Tempfile.new('.monetdb-')
        credentials << "user=#{config[:username]}\n"
        credentials << "password=#{config[:password]}"
        credentials.flush

        cmd = "DOTMONETDBFILE=#{credentials.path} mclient -Eutf-8 -h #{config[:host]} -d #{config[:database]} -s \"#{statement}\" - < #{file_path}"
        output = `#{cmd} 2>&1`
        if !$?.success?
          raise Error, "Bulk insert failed: #{output}"
        end
      ensure
        credentials.close
        credentials.unlink
      end
      output
    end

    def parse_value(type, value)
      unless value == "NULL"
        case type
        when :boolean
          parse_boolean_value value
        else
          super(type, value)
        end
      end
    end

    private

    QueryResult = Struct.new(:columns, :rows, :last_id)

    def parse_response(response)
      query_header, table_header = extract_headers!(response)

      case query_header[:type]
      when Q_TABLE
        QueryResult.new(table_header[:column_names].zip(table_header[:column_types]), parse_table_response(query_header, table_header, response))
      when Q_UPDATE
        QueryResult.new(nil, nil, query_header[:last_id])
      else
        true
      end
    end


    def to_query_header_hash(header)

      hash = {:type => header[1].chr}

      keys = {
        Q_TABLE => [:id, :rows, :columns, :returned],
        Q_BLOCK => [:id, :columns, :remains, :offset],
        Q_UPDATE => [:inserted, :last_id],
      }[hash[:type]]

      if keys
        values = header.split(" ")[1, 4].collect(&:to_i)
        hash.merge! Hash[keys.zip(values)]
      end

      hash.freeze
    end

  end
end




module Sequel
  module MonetDB

    AUTOINCREMENT = 'AUTO_INCREMENT'.freeze


    class Database < Sequel::Database

      SQL_BEGIN = "START TRANSACTION".freeze

      set_adapter_scheme :monetdb

      def connect(server)
        opts = server_opts(server)
        conn = ::MonetDB::Connection.new(opts)
        conn.connect
        conn
      end

      def disconnect_connection(c)
        c.disconnect
      end

      def connection_execute_method
        :query
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          conv = convert_sql(sql)
          r = nil
          begin
            r = log_yield(conv){conn.query(conv)}
            yield(r) if block_given?
          rescue Exception, ArgumentError => e
            puts "Error executing query: #{conv}"
            puts e.backtrace
            raise_error(e)
          end
          r
        end
      end

      def execute_insert(sql, opts=OPTS)
        execute(sql, opts){|c| return c.last_id}
      end

      # Return the number of matched rows when executing a delete/update statement.
      def execute_dui(sql, opts=OPTS)
        execute(sql, opts)
      end

      def bulk_load(table_name, file_path, delims, null_character, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            output = log_yield("Bulk load #{file_path} into #{table_name}"){ conn.bulk_load(table_name, file_path, delims, null_character) }
            log_info("Bulk load: #{output}")
            yield(r) if block_given?
          rescue Exception, ArgumentError => e
            `cp #{file_path} /var/tmp`
            raise_error(e)
          end
        end
      end

      # MonetDB doesn't require upcase identifiers.
      def identifier_input_method_default
        nil
      end

      # Disable the mitosis pipeline.
      # This pipeline generates parallel (multi-core) MAL instructions
      # to execute the query plan.
      # However, it generates highly non-optimal instructions in certain scenarios,
      # such as small range or point queries in large tables.
      def without_mitosis_pipeline
        ret = nil
        synchronize do
          begin
            run "set optimizer='no_mitosis_pipe'"
            ret = yield
          ensure
            run "set optimizer='default_pipe'"
          end
        end
        ret
      end

      private

      def auto_increment_sql
        AUTOINCREMENT
      end

      def begin_transaction_sql
        SQL_BEGIN
      end


      def convert_sql sql
        rewrite_neq_operator(sql)
      end

      def rewrite_neq_operator sql
        # ...a != b... ==> a <> b
        # since the former is not supported by Monet
        sql.gsub(/\!\=/, '<>')
      end

      def schema_parse_table table_name, opts = {}
        res = fetch "select c.* from \"tables\" t inner join columns c on c.table_id = t.id where t.name = '#{table_name}'"
        res.map do |row|
          row[:allow_null] = row.delete(:null) == true
          row[:default] = row.delete(:default)
          row[:db_type] = row.delete(:type)
          row[:type] = schema_column_type(row[:db_type])
          [row.delete(:name), row]
        end
      end


    end

    class Dataset < Sequel::Dataset

      Database::DatasetClass = self

      def bulk_load(file_path, delims, null_character)
        db.bulk_load(first_source_table, file_path, delims, null_character)
      end

      def fetch_rows(sql)
        execute(sql) do |s|
          i = -1
          cols = s.columns.map{|c| [output_identifier(c[0]), c[1], i+=1]}
          columns = cols.map{|c| c.at(0)}
          @columns = columns
          if rows = s.rows
            rows.each do |row|
              hash = {}
              cols.each{ |n,t,j| hash[n] = row[j] }
              yield hash
            end
          end
        end
        self
      end

      def data_size
        q = <<-SQL
          select
            sum(columnsize) as columns,
            sum(heapsize) as heap,
            sum(hashes) as hashes
          from storage
          where "schema" = 'sys' and "table" = '#{first_source_table}'
        SQL
        db.fetch(q).all
      end

    end
  end
end