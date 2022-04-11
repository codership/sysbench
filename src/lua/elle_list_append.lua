-- Copyright (C) 2022 Codership Oy <info@galeracluster.com>
--
-- Sysbench lua script to implement Elle list-append.
--
-- Maximum number of operations per transaction can be controlled
-- with --max-ops-per-trx. Transactions with only one operation are
-- run in autocommit mode if --use-autocommit=1 is given.
--
-- Each of the sysbench threads writes their history in the
-- separate file, prefixed with timestamp (usec resolution).
-- These files must be combined to get the final EDN file
-- which is readable with elle-cli (see below). Example bash
-- script to do this:
--
--   idx=1
--   sort -n -m list_append.*.tmp | \
--       while read line
--       do
--           echo "{:index $idx, $(echo $line | cut -f 1 --complement -d ' ')}"
--           ((idx+=1))
--       done > list_append.edn
--
--
-- Elle-cli: https://github.com/ligurio/elle-cli
--
-- Command line example for testing PL-2 level:
--
--        lein run \
--                  --model elle-list-append \
--                  --consistency-models PL-2 \
--                  --cycle-search-timeout 60000 \
--                  --directory ./out \
--                  --plot-timeout 60000 \
--                  ./list_append.edn
--

sysbench.cmdline.options = {
   rows = {"Number of rows", 10},
   read_lock_shared = {"Read use LOCK IN SHARE MODE", false},
   debug_on = {"Turn on debug pringing", false},
   max_ops_per_trx = {"Maximum number of operations per transaction", 3},
   max_op_rate = {"Maximum number of operations/sec/process", 100},
   use_autocommit = {"Run transactions with 1 op in autocommit mode", false}
}

-- Counter to generate values for append.
local counter = 0

-- Thread ID needs to be global in this unit to be able to write
-- histories from error handler before_restart_event().
local local_thread_id = 0

-- Node ID (local index) where this thread is connected to.
local node_id = nil

--
-- Helpers
--

-- Print debug logging if debug_on is enabled in options.
function print_debug(str)
   if sysbench.opt.debug_on then
      print(str)
   end
end

-- Get timestamp.
function get_timestamp()
   -- Function os.clock() returns the number of seconds of CPU time for the
   -- program, seemingly in microseconds (on Linux). Hopefully this is good
   -- enough to establish order of events between different threads.
   return os.clock() * 1000000
end

-- Sleep for given duration (in seconds)
function sleep(dur)
   os.execute("sleep " ..tonumber(dur))
end

-- Append an item to table/list.
function append_item(list, item)
   list[#list + 1] = item
end

-- Get value of status variable. Str parameter is query to get
-- the variable (SHOW [GLOBAL|SESSION] STATUS LIKE 'var').
function get_status(str)
   local result = con:query(str)
   assert(result.nrows == 1)
   local row = result:fetch_row()
   return row[2]
end
--
-- Schema preparation and cleanup.
--

-- Generate rows for table
function gen_rows()
   if sysbench.opt.rows == 0 then
      return
   end
   local t = { }
   append_item(t, string.format("INSERT INTO t VALUES (1, '')"))
   for i = 2, sysbench.opt.rows do
      append_item(t, string.format(", (%d, '')", i))
   end
   db_query(table.concat(t, ""))
end

-- Prepare database for load
function prepare()
   db_connect()
   db_query("CREATE TABLE IF NOT EXISTS t (k INT PRIMARY KEY, v TEXT) " ..
            "ENGINE=InnoDB")
   gen_rows()
   db_disconnect()
end

-- Clean up database
function cleanup()
   db_connect()
   db_query("DROP TABLE IF EXISTS t")
   db_disconnect()
end

--
-- Transaction generation.
--

-- This needs to be global to be able to print out failed transactions
-- in before restart event hook.
local current_invoke_list = { }
local complete_list = { }
local current_query = nil

-- Write a list of operations in a file
function write_list(thread_id, list, t, query, err)
   local file = assert(io.open("list_append." .. thread_id  ..".tmp", "a"))
   file:write(get_timestamp() .. " ")
   file:write(":type ", t, ", ")
   file:write(":process ", thread_id, ", ")
   file:write(":node ", node_id, ", ")
   file:write(":value [")
   for i = 1, #list do
      file:write("[ " .. list[i] .. "] ")
   end
   file:write("] ")
   if query then
      file:write(":query \"" .. query .. "\" ")
   end
   if err then
      file:write(":error " .. err)
   end
   file:write("\n")
   file:close()
end

-- Generate append query and operation string
function gen_append(thread_id, k)
   local v = string.format("%d", counter * sysbench.opt.threads + thread_id)
   counter = counter + 1
   local query = { key = nil, str = nil }
   query.str = string.format(
      "UPDATE t SET v = CONCAT(v, '%s', ' ') WHERE k = %d", v, k)
   local invoke = string.format(":append %d %s", k, v)
   return query, invoke
end

-- Generate read query and operation string
function gen_read(k)
   local invoke = string.format(":r %d nil", k)
   local query = { key = k, str = nil}
   query.str = string.format("SELECT v FROM t WHERE k = %d %s", k,
                             sysbench.opt.read_lock_shared and
                             "LOCK IN SHARE MODE" or "")
   return query, invoke
end

-- Generate queries
function gen_queries(thread_id)
   local query_list = { }
   local invoke_list = { }
   local ops = math.random(1, sysbench.opt.max_ops_per_trx)
   for i = 1, ops do
      local rnd = math.random(0, 1)
      local k = math.random(1, sysbench.opt.rows)
      local query = nil
      local invoke = nil
      if rnd > 0 then
         query, invoke = gen_append(thread_id, k)
      else
         query, invoke = gen_read(k)
      end
      append_item(query_list, query)
      append_item(invoke_list, invoke)
   end
   return query_list, invoke_list
end

-- Execute one query. Upon successful completion add the invoke operation
-- into complete_list.
function exec_query(query, invoke)
   print_debug("exec_query: " .. query.str)
   current_query = query.str
   local result = con:query(query.str)
   if query.key then
      assert(result.nrows == 1);
      local row = result:fetch_row();
      append_item(complete_list, string.format(":r %d [ %s ]", query.key, row[1]))
   else
      append_item(complete_list, invoke)
   end
end

-- Run one transaction.
function run_transaction(thread_id)
   local query_list, invoke_list = gen_queries(thread_id)
   assert(#query_list == #invoke_list)

   write_list(thread_id, invoke_list, ":invoke")
   current_invoke_list = invoke_list
   print_debug("START TRANSACTION: id: " .. thread_id .. ", queries: "
               .. #query_list)
   if not use_autocommit or #invoke_list > 1 then
      current_query = "START TRANSACTION"
      con:query("START TRANSACTION")
   end
   for i = 1, #query_list do
      -- Sleep random time between query execution to spread the
      -- duration of transactions a bit to increase interleaving.
      sleep((math.random()*2.)/sysbench.opt.max_op_rate)
      exec_query(query_list[i], invoke_list[i])
      invoke_list[i] = complete_list[i]
   end

   if not use_autocommit or #invoke_list > 1 then
      current_query = "COMMIT"
      con:query("COMMIT")
      print_debug("COMMIT: " .. thread_id)
   end
   write_list(thread_id, complete_list, ":ok")
   complete_list = { }
end

--
-- Sysbench thread/event hooks.
--

-- Initialize thread execution context.
function thread_init(thread_id)
   math.randomseed(os.time() + thread_id)
   drv = sysbench.sql.driver()
   con = drv:connect()
   local_thread_id = thread_id -- For before_restart_event hook
   node_id = get_status("SHOW STATUS LIKE 'wsrep_local_index'")
   print_debug("Thread " .. thread_id .. " connected to node " .. node_id)
end

-- Deinitialize thread execution context.
function thread_done(thread_id)
   con:disconnect()
end

function event(thread_id)
   -- con:reconnect()
   run_transaction(thread_id)
end

-- Handle ignorable errors
function sysbench.hooks.before_restart_event(errdesc)
   print_debug("Thread: " .. local_thread_id .. " Query: "
               .. (current_query or "(nil)") .. " error " .. errdesc.sql_errno)
   if errdesc.sql_errno == 1213    -- ER_LOCK_DEADLOCK
      or errdesc.sql_errno == 1180 -- ER_ERROR_DURING_COMMIT
      or errdesc.sql_errno == 1205 -- ER_LOCK_WAIT_TIMEOUT
   then
      con:query("ROLLBACK")
      print_debug("ROLLBACK: " .. local_thread_id)
   end
   write_list(thread_id, current_invoke_list, ":fail", current_query,
              errdesc.sql_errno)
   current_invoke_list = { }
   complete_list = { }
   current_query = nil
end
