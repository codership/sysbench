pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")
dofile(pathtest .. "galera_common.lua")

function thread_init(thread_id)
   set_vars()
   sync_wait = 1
end

function event(thread_id)
   db_query("SET SESSION WSREP_SYNC_WAIT = " .. sync_wait)
   db_query("START TRANSACTION")
   do_galera_transaction()
   db_query("COMMIT")
   db_query("SET SESSION WSREP_SYNC_WAIT = " .. 0)
end
