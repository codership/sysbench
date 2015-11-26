function do_galera_transaction()
   local c_val
   local table_name
   table_name = "sbtest".. sb_rand_uniform(1, oltp_tables_count)
   rs = db_query("SELECT id FROM ".. table_name .." WHERE id=" .. sb_rand(1, oltp_table_size))
   rs = db_query("UPDATE ".. table_name .." SET k=k+1 WHERE id=" .. sb_rand(1, oltp_table_size))
end
