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

        cmd = "DOTMONETDBFILE=#{credentials.path} mclient -h #{config[:host]} -d #{config[:database]} -s \"#{statement}\" - < #{file_path}"
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

    ResultSet = Struct.new(:columns, :rows)

    def parse_response(response)
      query_header, table_header = extract_headers!(response)
      if query_header[:type] == Q_TABLE
        ResultSet.new(table_header[:column_names].zip(table_header[:column_types]), parse_table_response(query_header, table_header, response))
      else
        true
      end
    end

  end
end




module Sequel
  module MonetDB

    AUTOINCREMENT = 'AUTO_INCREMENT'.freeze


    class Database < Sequel::Database
      set_adapter_scheme :monetdb

      GUARDED_DRV_NAME = /^\{.+\}$/.freeze
      DRV_NAME_GUARDS = '{%s}'.freeze
      DISCONNECT_ERRORS = /\A08S01/.freeze

      def connect(server)
        opts = server_opts(server)
        conn = ::MonetDB::Connection.new(opts)
        conn.connect
        conn
      end

      def disconnect_connection(c)
        c.disconnect
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            conv = convert_sql(sql)
            puts "Executing query: #{conv}"
            r = log_yield(sql){conn.query(conv)}
            yield(r) if block_given?
          rescue Exception, ArgumentError => e
            raise_error(e)
          end
          nil
        end
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
            raise_error(e)
          end
        end
      end


      private

      def auto_increment_sql
        AUTOINCREMENT
      end


      def convert_sql sql
        sql = sql.downcase
        sql = remove_not_equal(sql)
        sql
      end

      def remove_not_equal sql
        # ...(a != b)... ==> NOT(a == b)
        # since the former is not supported by Monet
        sql.gsub(/\(?([\'\"]?[\w].?[\w]+[\'\"]?)\)?\s?\!\=\s?([\'\"]?[\w].?[\w]+[\'\"]?)\)?/, 'not (\1 = \2)')
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

    end
  end
end
