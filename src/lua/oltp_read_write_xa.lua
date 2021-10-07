#!/usr/bin/env sysbench

-- Copyright (C) 2021 Codership Oy <info@codership.com>
-- Read/write OLTP benchmark using XA transactions

require("oltp_common")

function prepare_xa_statements()
   local xid = "'xid-" .. sysbench.tid .. "'"
   stmt.xa_start = con:prepare("XA START " .. xid)
   stmt.xa_end = con:prepare("XA END " .. xid)
   stmt.xa_prepare = con:prepare("XA PREPARE " .. xid)
   stmt.xa_commit = con:prepare("XA COMMIT " .. xid)
   stmt.xa_rollback = con:prepare("XA ROLLBACK " .. xid)
end

function prepare_statements()
   prepare_xa_statements()

   prepare_point_selects()

   if sysbench.opt.range_selects then
      prepare_simple_ranges()
      prepare_sum_ranges()
      prepare_order_ranges()
      prepare_distinct_ranges()
   end

   prepare_index_updates()
   prepare_non_index_updates()
   prepare_delete_inserts()
end

function xa_start()
   stmt.xa_start:execute()
end

function xa_end()
   stmt.xa_end:execute()
end

function xa_prepare()
  stmt.xa_prepare:execute()
end

function xa_commit()
   stmt.xa_commit:execute()
end

function xa_rollback()
   stmt.xa_rollback:execute()
end

function event()
   xa_start()

   execute_point_selects()

   if sysbench.opt.range_selects then
      execute_simple_ranges()
      execute_sum_ranges()
      execute_order_ranges()
      execute_distinct_ranges()
   end

   execute_index_updates()
   execute_non_index_updates()
   execute_delete_inserts()

   xa_end()
   xa_prepare()
   xa_commit()
end

function ignorable_error(sql_errno)
   if sql_errno == 1213 or   -- ER_LOCK_DEADLOCK
      sql_errno == 1397 or   -- ER_XAER_NOTA
      sql_errno == 1399 or   -- ER_XAER_RMFAIL
      sql_errno == 1614      -- ER_XA_RBDEADLOCK
   then
      return true
   else
      return false
   end
end

function check_restart_error(err)
   if type(err) ~= "table" or
      err.errcode ~= sysbench.error.RESTART_EVENT or
      not ignorable_error(err.sql_errno)
   then
      if type(err) == "table" then
         print("error code " .. err.errcode)
         print("sql errno  " .. err.sql_errno)
      end
      error(err, 2)
   end
end

-- This hook is called when an ignorable error
-- is raised during processing of an event.
-- Here we need to explicityl rollback the current
-- transaction using XA END followed by XA ROLLBACK,
-- before starting the next transaction.
-- Notice that the caller of this hook (thread_run()
-- in sysbench.lua) does not handle errors.
-- So here we call xa_end() and xa_rollback()
-- through pcall() to catch and handle potential errors.
function sysbench.hooks.before_restart_event(errdesc)
   local success, ret = pcall(xa_end)
   if not success then
      check_restart_error(ret)
   end
   local success, ret = pcall(xa_rollback)
   if not success then
      check_restart_error(ret)
   end
end
