pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")
dofile(pathtest .. "galera_common.lua")

function thread_init(thread_id)
   set_vars()
   sync_wait_upto = "00000000-0000-0000-0000-000000000000:-1"
end

function event(thread_id)
   db_query("SELECT WSREP_SYNC_WAIT_UPTO('" .. sync_wait_upto .. "')")
   db_query("START TRANSACTION")
   do_galera_transaction()
   db_query("COMMIT")
   sync_wait_upto = db_last_committed_id(0)
end
