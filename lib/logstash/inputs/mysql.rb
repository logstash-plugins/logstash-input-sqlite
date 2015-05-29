# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket"


# Read rows from an mysql database.
#
# This is most useful in cases where you are logging directly to a table.
# Any tables being watched must have an change column that is increasing in value.
# This change column can be an int, timestamp, datetime or intastimestamp(bigint).
#
# 
#
# Example
# Then with this logstash config:
# [source,ruby]
#     input {
#       mysql {
#         host => "yourhost.atyourdomain"
#         db => "yourdb, yourdb2"
#         username => "user"
#         password => "pass"
#         changecol => "id"
#         changecoltype => "int"
#       }
#     }
#     output {
#       stdout {
#         debug => true
#       }
#     }
#
# Sample output:
# [source,ruby]
class LogStash::Inputs::Mysql < LogStash::Inputs::Base
  config_name "mysql"
  plugin_status 1
#
  # The host of the MySql Server
  config :host, :validate => :string, :default => "mspdvdseeprs11"
 
  # The port of the MySql Server.
  config :port, :validate => :number, :default => 3306
  
  # The database or databases to read from
  config :db, :validate => :array, :required => true, :default => "STG_000075" 
    
  # A hash containing any MYSQL jdbc properties you wish to pass into the connection.
  config :connproperties, :validate => :hash, :default => {}
  
  # The username for the mysql database
  config :username, :validate => :string, :default => ""
  
  # The password for the mysql database
  config :password, :validate => :string, :default =>  ""
 
  # The tables to read from. If empty it will read from all tables in the schema/db.
  config :tables, :validate => :array, :default => [""] #

  # The columns to select. If empty it will select all columns.
  config :columns, :validate => :array, :default => [["",""]] #[[""]]#
    
  # Option to repeat a select with the last returned changecol value. Allows for non distinct changecol values, such as batches.
  config :repeatlast, :validate => :boolean, :default => false
  
  # Repeat last column to check for new rows. Set this to a distinct value such as a UUID.
  config :repeatlast_checkcolumn, :validate => :string, :default => ""
  
  # The minimum time to sleep in between queries.
  config :sleepmin, :validate => :number, :default => 5
  
  # The maximum time to sleep in between queries. When the sleep time reaches this value it will reset to the minimum.
  config :sleepmax, :validate => :number, :default => 30
  
  # The change column. This column is tracked so to not repeat rows.
  config :changecol, :validate => :string, :required => true, :default => ""
   
  # The type of the change column. Currently supported types are int, timestamp, datetime and intastimestamp(bigint).
  config :changecoltype, :validate => :string, :required => true, :default => "intastimestamp"
   
  # The format for the change column. Default unused.
  config :changecolformat, :validate => :string, :default => ""
   
  SINCE_TABLE = :since_table
  TIMEOUT = 20
  RETRY_TIMEOUT = 2
  CHANGECOL_TYPES = [:int, :datetime, :timestamp, :intastimestamp]
    
  $changecol_type = ""
  $curlastread
  $newlastread
  $curdb
 
  # The keystore file used for the SSL connection.
   config :keystore, :validate => :string, :default => ""
   
  # The trust store file used for the SSL connection.
   config :truststore, :validate => :string, :default =>""

   public
   def register
    check_settings()
     # Load all drivers
    # Dir["C:\logstash-1.5.0.beta1\vendor\bundle\jruby\1.9\gems\jdbc-mysql-5.1.33\lib*.jar"].each  {|jar| require jar} 
    require "java"
     require "jdbc/mysql"
     Jdbc::MySQL.load_driver
     
     connect(@db[0])
     if !@db.empty?
      @db.each do |database|
        #puts "Init table"
        init_placeholder_table(database)
      end
    end
  end
#
  def connect(dbn)

     begin
       @curdb = dbn
     
       if !@username.empty? && !@password.empty? && !@truststore.empty? && !@keystore.empty?
         begin
           url = "jdbc:mysql://#{@host}:#{@port}/#{dbn}"
           props = java.util.Properties.new
           props.setProperty 'user', @username
           props.setProperty 'password', @password
           props.setProperty 'useSSL', 'false'
           props.setProperty 'verifyServerCertficate', 'false' 
            if !@connproperties.empty?
              @connproperties.each do |key,value|
                  props.setProperty key, value
              end    
            end
           #props.setProperty 'javax.net.ssl.keyStore', cert
           #props.setProperty 'javax.net.ssl.TrustStore', key
           java.lang.System.setProperty 'javax.net.ssl.trustStore', @truststore
           #java.lang.System.setProperty 'javax.net.ssl.trustStorePassword', 'changeit'
 
           java.lang.System.setProperty 'javax.net.ssl.keyStore', @keystore
           #java.lang.System.setProperty 'javax.net.ssl.keyStorePassword', 'changeit'
            
           @conn= Java::com.mysql.jdbc.Driver.new.connect(url, props)
           #puts @conn.getSchema()
           #puts @conn.getCatalog()
           #puts @conn.getMetaData()
            
           #puts "connection successful"
         rescue => e
           #puts "#{e.class.name}: #{e.message}"
           #puts "connection failed" 
           @logger.debug("#{e.class.name}: #{e.message}")
           @logger.debug("Connection Failed") 
         end
      elsif !@username.empty? && !@password.empty? && !@connproperties.empty?
         begin
           props = java.util.Properties.new
                     props.setProperty 'user', @username
                     props.setProperty 'password', @password
           @connproperties.each do |key,value|
             props.setProperty key, value
           end     
           url = "jdbc:mysql://#{@host}:#{@port}/#{dbn}"
           @conn= Java::com.mysql.jdbc.Driver.new.connect(url, props)
           #puts "connection successful"
         rescue => e
           #puts "#{e.class.name}: #{e.message}"
          # puts "connection failed"
           @logger.debug("#{e.class.name}: #{e.message}")
           @logger.debug("Connection Failed")
         end
         elsif !@username.empty? && !@password.empty? && @connproperties.empty?
            begin
              url = "jdbc:mysql://#{@host}:#{@port}/#{dbn}"
              props = java.util.Properties.new
                                  props.setProperty 'user', @username
                                  props.setProperty 'password', @password
              @conn= Java::com.mysql.jdbc.Driver.new.connect(url, props)
              #@conn= java.sql.DriverManager.get_connection(url)
              puts "connection successful"
            rescue => e
              puts "#{e.class.name}: #{e.message}"
              puts "connection failed"
              @logger.debug("#{e.class.name}: #{e.message}")
              @logger.debug("Connection Failed")
            end
         else
        begin
          url = "jdbc:mysql://#{@host}:#{@port}/#{dbn}"
          @conn= java.sql.DriverManager.get_connection(url)
          #puts "connection successful"
        rescue => e
          #puts "#{e.class.name}: #{e.message}"
          #puts "connection failed"
          @logger.debug("#{e.class.name}: #{e.message}")
          @logger.debug("Connection Failed")
        end
      end
     end
  end

  public 
  def change_database(db)
    if db != nil
       begin
         @conn.setCatalog(db)
         @curdb = db 
       rescue => e
          #sqlState = e.getSQLState();
          reconnect(db)
          #puts "#{e.class.name}: #{e.message}"
          @logger.debug("Change Database Error #{e.class.name}: #{e.message}")
       end
    else
      db = ""
     end
   end
   
#   public def
#   change_schema(db)
#    if db != nil
#       begin
#         @conn.setSchema(db)
#         @curdb = db
#       rescue => e
#          #sqlState = e.getSQLState();
#          reconnect(db)
#          #puts "#{e.class.name}: #{e.message}"
#          @logger.debug("Change Database Error #{e.class.name}: #{e.message}")
#       end
#      else
#        db = ""
#    end
#  end
#  
public
  def check_settings
  if @changecol.empty? 
    #puts "The change tracking column was not set."
   @logger.debug("A change tracking column was not set.")
   java.lang.System.exit(0)
 
  elsif @changecoltype.empty? 
    #puts "The change tracking column type was not set. Options are datetime, timestamp, int, intastimestamp"
   @logger.debug("The change tracking column type was not set. Options are datetime, timestamp, int, intastimestamp")
   java.lang.System.exit(0)
  else
    found = false
    typeslist = ""
    CHANGECOL_TYPES.each do |type|
      if !typeslist.empty? 
        typeslist += ", "
      end
      typeslist += "#{type}"
      if "#{@changecoltype}" == "#{type}"
        found = true
      end
    end
    if !found
      #puts "A change tracking column type was not set. Options are #{typeslist}."
      @logger.debug("A change tracking column type was not set. Options are #{typeslist}.")
      java.lang.System.exit(0)
    end
  end
  if @db.empty?
    #puts "No Database/Schema was set! Please set the db property."
    @logger.debug("No Database/Schema was set! Please set the db property.")
    java.lang.System.exit(0)
  end
 end
  
 
public
  def reconnect(db)
    if @conn != nil
      if @conn.isClosed() 
        retryCount = 0
        while retryCount < TIMEOUT && @conn.isClosed()  
          begin
            connect(db)
          rescue => e
             #puts "#{e.class.name}: #{e.message}"
             @logger.debug("Unable to Reconnect #{e.class.name}: #{e.message}. Retry: #{retryCount} Of #{TIMEOUT}")
          end
          retryCount += 1
        end
      end
    end
  end
    
public
  def init_placeholder_table(db)
    CHANGECOL_TYPES.each do |type|
       if "#{@changecoltype}" == "#{type}"
        
         if "#{type}" == "intastimestamp"
            type = "bigint"
         end
         @changecol_type = type
         begin
           sql = "CREATE TABLE IF NOT EXISTS #{db}.#{SINCE_TABLE} (tablename varchar(48), place #{type}, PRIMARY KEY(tablename))"
           #puts sql
           @conn.createStatement.execute sql
         rescue => e
           #sqlState = e.getSQLState();
           reconnect(db)
           #puts "#{e.class.name}: #{e.message}"
           @logger.debug("Create Since_Table Error #{e.class.name}: #{e.message}")
         end
       end
    end
 end
 
public
   def select_rows(sql)
     begin
       resultSet = @conn.createStatement.executeQuery sql
       rows = []
       meta = resultSet.getMetaData
       column_count = meta.getColumnCount
       while resultSet.next
         res = {}

         for i in 1..column_count
           name = meta.getColumnName i
           case meta.getColumnType i
             when java.sql.Types::INTEGER
               res[name] = resultSet.getInt name
             when java.sql.Types::TIMESTAMP
               res[name] = resultSet.getTimestamp name
             when java.sql.Types::DATE
               res[name] = resultSet.getDate name
             else
               res[name] = resultSet.getString name
           end
           if name == @changecol
             if @curlastread < res[name]
               @newlastread = res[name]
             end
           end
         end
         rows << res
       end
       
     rescue => e
        #String sqlState = e.getSQLState();
        reconnect(db)
       puts "#{e.class.name}: #{e.message}"
       @logger.debug("Error #{e.class.name}: #{e.message}")
     end
     return rows
   end

   
public
   def query_table(table,columns,lastread)
     begin
       #statement = session.prepare('INSERT INTO users (username, email) VALUES (?, ?)')
       #session.execute(statement, 'avalanche123', 'bulat.shakirzyanov@datastax.com')
       #sql.sub(" > '#{lastread}'", " = '#{newlastread}'")
       sql = "SELECT "
       
       if !@columns.empty? 
         cbi = @db.find_index(@curdb) 
         if cbi != nil && @columns.count >= cbi
          g = 0
           @columns.each do |group|
             if g = cbi
               if group.find_index(@changecol) == nil
                 sql += " " + "#{@changecol}" + ""
               end
               group.each do |col|
                sql += ", " + "#{col}" + ""
               end
             end
             g += 1             
           end
         else
           sql += "*"
         end
       else
           sql += "*"
       end
       sql += " FROM " + "#{table}" + " WHERE " + "#{@changecol}" + " > '#{lastread}'"
       puts sql
       rows = []
       
       rows += select_rows(sql)
      if rows.count > 0
        if "#{@changecoltype}" == "intastimestamp" || retrylast
         sql = sql.sub(" > '#{lastread}'", " = '#{@newlastread}'")
         
         c = find_changecol_count(rows,@newlastread)
         
         timeout = RETRY_TIMEOUT
         
         while timeout > 0
           sleep(@sleepmin)
           newrows = select_rows(sql)
           if @repeatlast_checkcolumn.empty?
             if !@columns.empty?
               cbi = @db.find_index(@curdb) 
               if @columns.count >= cbi
                  arr= @columns[cbi]
               @repeatlast_checkcolumn = arr[0]
               end
             end
           end
           
           if newrows.count > c && !@repeatlast_checkcolumn.empty?
              rows = add_new_rows(rows,newrows,@repeatlast_checkcolumn,@newlastread)
              c += (newrows.count - c)
           elsif !@repeatlast_checkcolumn.empty?
             timeout -= 1
           else
             timeout = 0
           end
         end
        end
      end
     end
         
     return rows
   end
   
public
 def find_changecol_count(rows, value)
   c = 0
   rows.each do |row| 
       if row["#{@changecol}"] == value
         c += 1
       end
   end
   return c
 end

public
 def add_new_rows(rows, newrows, rcheck, value)
   c = 0
   found = false
   newrows.each do |nrow| 
     rows.each do |orow| 
       if orow["#{@changecol}"] == value
         if orow["#{rcheck}"] == nrow["#{rcheck}"]
               found = true
         end
       end
     end
     if found
       found = false
     else
       rows << nrow
     end
   end
   return rows
 end
 
 public
 def get_last_read(table)
   value = nil
   str = "SELECT place FROM #{SINCE_TABLE} WHERE tablename = '#{table}' "
   #puts str
   rs = 0
   begin
     resultSet = @conn.createStatement.executeQuery str
     while resultSet.next
       value = resultSet.getString "place"
       rs += 1
     end
   rescue => e
          #sqlState = e.getSQLState();
          reconnect(db)
         #puts "#{e.class.name}: #{e.message}"
         @logger.debug("Error #{e.class.name}: #{e.message}")
   end
   if rs < 1
     
     if "#{@changecol_type}" == "int"
       value = 0
       add_to_placeholder(table,value)
     elsif "#{@changecol_type}" == "datetime"
       value = DateTime.now()
       if !@changecolformat.empty?
         add_to_placeholder(table,value.strftime(@changecolformat))
       else
         add_to_placeholder(table,value)
       end
     elsif "#{@changecol_type}" == "timestamp"
       value = Time.now()
       if !@changecolformat.empty?
         add_to_placeholder(table,value.strftime(@changecolformat))
       else
         add_to_placeholder(table,value)
       end
     elsif"#{@changecol_type}" == "bigint"
       value = Time.now()
       if !@changecolformat.empty?
         add_to_placeholder(table,value.strftime(@changecolformat))
       else
         add_to_placeholder(table,value.strftime("%Y%m%d%H%M"))
       end
     end
   end
   return value
 end
 
 public
 def add_to_placeholder(table,value)
   begin
      #puts "adding to placeholder"
      str = "INSERT INTO #{SINCE_TABLE} (tablename, place) VALUES ('#{table}', '#{value}')"
      @conn.createStatement.execute str
   rescue => e
     #sqlState = e.getSQLState();
        reconnect(db)
      #puts "#{e.class.name}: #{e.message}"
      @logger.debug("Error #{e.class.name}: #{e.message}")
   end
 end
 
 public
 def update_placeholder(table,value)
   begin
     str = "UPDATE #{SINCE_TABLE} SET place = '#{value}' WHERE tablename = '#{table}'"
     @conn.createStatement.execute str
   rescue => e
     #sqlState = e.getSQLState();
        reconnect(db)
     #puts "#{e.class.name}: #{e.message}"
     @logger.debug("Error #{e.class.name}: #{e.message}")
   end
 end
 
public
   def run(queue)
     #require "java"
     #require 'jdbc/mysql'

    sleeptime = @sleepmin

      loop do
        begin
        rowcount = 0
        dbcount = 0
        @logger.debug("DB: #{db}") 
        @db.each do |database|
          @logger.debug("DB:. #{database}")
          if dbcount > 0
            
            change_database(database)
          end
            if @tables.empty?
              all = ['*']
              @curlastread = get_last_read(table)
              @newlastread = @curlastread
              DatabaseMetaData md = @conn.getMetaData();
              rs = md.getTables(null, null, "%", null);
              while rs.next
                query_table(rs.getString(3),all,@curlastread)
              end
              
              #puts "Rows:" +rows.count
              rowcount += rows.count
              rows.each do |row| 
                   event = LogStash::Event.new("database" => database, "table" => "all")
                   decorate(event)
                   # store each column as a field in the event.
                   row.each do |column, element|
                     next if column == :id
                     event[column.to_s] = element
                   end
                   queue << event
               end
               
               if @newlastread > @curlastread
                   @curlastread = @newlastread
                   update_placeholder(table,@curlastread)
                end
            else
              
              @tables.each do |table|
                @curlastread = get_last_read(table)
                @newlastread = @curlastread
                rows = query_table(table,@columns,@curlastread)
                #puts "Rows: #{rows.count}"
                rowcount += rows.count
                rows.each do |row| 
                     event = LogStash::Event.new("database" => database, "table" => table)
                     decorate(event)
                     # store each column as a field in the event.
                     row.each do |column, element|
                       next if column == :id
                       event[column.to_s] = element
                     end
                     queue << event
                end #Rows loop
                
                if @newlastread > @curlastread
                   @curlastread = @newlastread
                  update_placeholder(table,@curlastread)
                end
                
              end # Table Loop
           end # Table if
           if rowcount == 0
              # nothing found in that iteration
              # sleep a bit
             #puts "No new rows. Sleeping."
              @logger.debug("No new rows. Sleeping.", :time => sleeptime)
              sleeptime = [sleeptime * 2, @sleepmax].min
              sleep(sleeptime)
            else
              sleeptime = @sleepmin
            end
            dbcount += 1
         end # database loop
     rescue LogStash::ShutdownSignal
      @conn.close()
    break
    end # begin/rescue
  end # loop
#    @conn.close()
 end #run
end # class Logtstash::Inputs::EventLog

#c = LogStash::Inputs::Mysql2.new()
#c.register
#queue = SizedQueue.new(20)
#c.run(queue)
