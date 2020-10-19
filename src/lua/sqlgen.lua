-- Copyright (C) 2020 Codership Oy <info@codership.com>
--
-- This script implements load generator similar to sqlgen tool.
--
-- The load is based on matrix rotations in order to make
-- queries non-commutative.
--


-- Command line options
sysbench.cmdline.options = {
   rows = {"Number of rows per table.", 4},
   max_rows = {"Don't allow tables growing over this number of rows.", 4},
   tables = {"Number of tables.", 4},
   secondary = {"Create secondary unique index.", false},
   ac_frac = {"Fraction of autocommit queries.", 0.},
   trans_min = {"Minimum transaction length in queries.", 5},
   trans_max = {"Maximum transaction length in queries.", 10},
   selects = {"Weight of select queries in the load.", 70},
   updates = {"Weight of update queries in the load.", 20},
   inserts = {"Weight of insert queries in the load.", 6},
   -- replaces = {"Weight of replace queries in the load.", 6},
   deletes = {"Weight of delete queries in the load.", 4},
   rollbacks = {"Fraction of rollbacks instead of commits.", 0.1},
   unique = {"Create unique index on table.", false},
   -- update_primary = {"Updates change primary column.", false},
   -- update_unique = {"Updates change unique column.", false},
   reconnect_frac = {"Fraction of transactions which cause reconnect.", 0.01}
}


function gen_rows(tab)
   if sysbench.opt.rows == 0 then
      return
   end
   local i
   local t = { }
   t[1] = string.format("INSERT INTO %s VALUES (1, 1, 1, 1, 1)", tab)
   for i = 2, sysbench.opt.rows do
      t[#t + 1] = string.format(", (%d, 1, 1, 1, %d)", i ,i)
   end
   db_query(table.concat(t, ""))
end

function prepare()
   local i
   db_connect()
   for i = 1, sysbench.opt.tables do
      tab = string.format("comm%d", i)
      unique_key = sysbench.opt.unique and "UNIQUE KEY" or ""
      create_str = string.format(
         [[CREATE TABLE IF NOT EXISTS %s
            (p INT PRIMARY KEY, x INT, y INT, z INT, u INT %s)]],
         tab, unique_key)
      print(create_str)
      db_query(create_str)
      gen_rows(tab)
   end
   db_disconnect()
end

function cleanup()
   local i
   db_connect()
   for i = 1, sysbench.opt.tables do
      db_query(string.format("DROP TABLE IF EXISTS comm%d", i))
   end
   db_disconnect()
end

-- Weights of queries, see query_ops table below.
query_weights = { }
query_distribution = { }

function compute_weights(thread_id)
   local i
   local so = sysbench.opt
   local w = {}
   w[1] = so.selects
   w[2] = so.inserts
   w[3] = so.updates
   w[4] = so.deletes

   local total_weight = 0.0
   for i = 1, 4 do
      total_weight = total_weight + w[i]
   end

   if total_weight == 0 then
      error("Some of selects, inserts, updates, deletes must be non-zero.")
   end

   for i = 1, 4 do
      query_weights[i] = w[i]/total_weight
   end

   for i = 1, 4 do
      query_distribution[i] = 0
   end

   print("Thread", thread_id, query_weights[1], query_weights[2], query_weights[3], query_weights[4])
end

function print_query_distribution(thread_id)
   local total = 0.0
   for i = 1, 4 do
      total = total + query_distribution[i]
   end
   print("Thread", thread_id,
         query_distribution[1]/total,
         query_distribution[2]/total,
         query_distribution[3]/total,
         query_distribution[4]/total)
end

function thread_init(thread_id)
   drv = sysbench.sql.driver()
   con = drv:connect()
   compute_weights(thread_id)
end

function thread_done(thread_id)
   print_query_distribution(thread_id)
end

function begin()
   con:query("BEGIN")
end

function commit()
   con:query("COMMIT")
end

function rollback()
   con:query("ROLLBACK")
end

function random_table()
   return string.format("comm%d", math.random(1, sysbench.opt.tables))
end

function random_pk()
   return math.random(1, sysbench.opt.max_rows)
end

function random_uk()
   return math.random(1, sysbench.opt.max_rows)
end

function select_fun()
   tab = random_table()
   pk = random_pk()
   select_str = string.format("SELECT p, x, y, z, u FROM %s WHERE p = %d",
                              tab, pk)
   con:query(select_str)
end

function insert_fun()
   tab = random_table()
   pk = random_pk()
   uk = random_uk()
   con:query(string.format("INSERT INTO %s VALUES (%d, %d, %d, %d, %d)",
                           tab, pk, 1, 1, 1, uk))
end

function update_unique()
   return string.format(", u = %d", random_uk())
end

function update_where()
   return string.format("WHERE p = %d", random_pk())
end

function update_fun()
   tab = random_table()
   rnd = math.random(1, 65536)
   update_str = string.format([[UPDATE %s SET
          x = (x + 1) %% 65537,
          y = ((y * x) %% 65537 + (%d * y) %% 65537 + (y * z) %% 65537) %% 65537,
          z = ((z * x) %% 65537 + (z * y) %% 65537 + (%d * z) %% 65537) %% 65537
          %s %s]], tab, rnd, rnd, update_unique(), update_where())
   con:query(update_str)
end

function delete_where()
   return string.format("WHERE p = %d", random_pk())
end

function delete_fun()
   tab = random_table()
   where = delete_where()
   delete_str = string.format("DELETE FROM %s %s", tab, where)
   con:query(delete_str)
end

query_ops = {
   select_fun,
   insert_fun,
   update_fun,
   delete_fun
}

-- Produces random query type with distribution given by
-- query_weights.
function query_type()
   rnd = math.random()
   for i = 1, 4 do
      if rnd < query_weights[i] then
         query_distribution[i] = query_distribution[i] + 1
         return i
      end
      rnd = rnd - query_weights[i]
   end
   assert(nil)
end

function run_query()
   query_ops[query_type()]()
end

function run_transaction()
   begin()
   for i = sysbench.opt.trans_min, sysbench.opt.trans_max do
      run_query()
   end
   commit()
end


function event()
   if math.random() < sysbench.opt.reconnect_frac then
      con:reconnect()
   end
   if math.random() < sysbench.opt.ac_frac then
      run_query()
   else
      run_transaction()
   end
end

function print_count(con, tab)
   local rs = con:query(
      string.format("SELECT COUNT(*) FROM %s", tab))
   assert(rs.nrows == 1)
   local row = rs:fetch_row()
   print("Number of rows in table: ", tab, row[1])
end

function check_distinct(con, tab, col, prim_sec)
   local rs = con:query(
      string.format("SELECT COUNT(*) = COUNT(DISTINCT %s) FROM %s", col, tab))
   assert(rs.nrows == 1)
   local row = rs:fetch_row()
   local val = tonumber(row[1])
   if val ~= 1 then
      error(string.format(
               "Rows count does not match distinct keys count for: %s",
               prim_sec))
   else
      print("Keys ok:", prim_sec)
   end
end

function check_fun()
   local i, row
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      local tab = string.format("comm%d", i)
      print_count(con, tab)
      check_distinct(con, string.format("comm%d", i), "p", "PRIMARY")
      if sysbench.opt.unique then
         check_distinct(con, string.format("comm%d", i), "u", "UNIQUE")
      end
   end
   con:disconnect()
end

sysbench.cmdline.commands = {
   check = {check_fun}
}
