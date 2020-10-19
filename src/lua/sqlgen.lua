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
   replaces = {"Weight of replace queries in the load.", 6},
   deletes = {"Weight of delete queries in the load.", 4},
   rollbacks = {"Fraction of rollbacks instead of commits.", 0.1},
   unique = {"Create unique index on table.", false},
   update_primary = {"Updates change primary column.", false},
   update_unique = {"Updates change unique column.", false},
   reconnect_frac = {"Fraction of transactions which cause reconnect.", 0.01}
}


function gen_rows(tab)
   local i
   for i = 1, sysbench.opt.rows do
      insert_str = string.format("INSERT INTO %s VALUES (%d, 1, 1, 1, %d)",
                             tab, i, i)
      print(insert_str)
      db_query(insert_str)
   end
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

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
   total_weight = 
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

function update_where()
   return string.format("WHERE p = %d", random_pk())
end

function update_fun()
   tab = random_table()
   rnd = math.random(1, 65536)
   where = update_where()
   update_str = string.format([[UPDATE %s SET
          x = (x + 1) %% 65537,
          y = ((y * x) %% 65537 + (%d * y) %% 65537 + (y * z) %% 65537) %% 65537,
          z = ((z * x) %% 65537 + (z * y) %% 65537 + (%d * z) %% 65537) %% 65537
          %s]], tab, rnd, rnd, where)
   con:query(update_str)
end

function delete_where()
   return string.format("WHERE p = %d", random_pk())
end

function delete_fun()
   tab = random_table()
   where = delete_where()
   delete_str = string.format("DELETE FROM %s %s", tab, where)
   print(delete_str)
   con:query(delete_str)
end

query_ops = {
   select_fun,
   insert_fun,
   update_fun,
   delete_fun
}

function run_autocommit()
   local query_type = math.random(1, 4)
   query_ops[query_type]()
end

function run_transaction()
   begin()

   commit()
end


function event()
   if math.random() < sysbench.opt.reconnect_frac then
      con:reconnect()
   end
   if math.random() < sysbench.opt.ac_frac then
      run_autocommit()
   else
      run_transaction()
   end
end
