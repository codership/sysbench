pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")
dofile(pathtest .. "galera_common.lua")

function thread_init(thread_id)
   set_vars()
end

function event(thread_id)
   db_query("START TRANSACTION")
   do_galera_transaction()
   db_query("COMMIT")
end
