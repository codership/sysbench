-- Copyright (C) Codership Oy <info@codership.com>
--
-- A benchmark to run updates on text/blob column in order
-- to generate large write sets.

function help()
   print("A benchmark to run updates on text/blob column in order")
   print("to generate large write sets. Each event corresponds to")
   print("two updates.")
   print("\n")
   print("Test parameters:")
   print("--update-blob-pad-size <pad_size> - Size of blob to be updated, default 100k")
   print("\n")
end

function thread_init(thread_id)
   update_blob_pad_size = update_blob_pad_size or 131072
end

function prepare()
   local i
   db_connect()
   db_query("CREATE TABLE IF NOT EXISTS update_blob (id INT PRIMARY KEY, data LONGTEXT)")
   for i = 0, num_threads - 1 do
      db_query("INSERT INTO update_blob VALUES (" .. i ..
		  ", LPAD('x', " .. update_blob_pad_size .. ", 'x'))")
   end
   db_disconnect()
end

function cleanup()
   db_query("DROP TABLE IF EXISTS update_blob")
end

function execute_update(thread_id, pad_char, pad_size)
   db_query("UPDATE update_blob SET data=LPAD('" .. pad_char ..
	       "', " .. pad_size .. ", '" .. pad_char ..
	       "') WHERE id = " .. thread_id)
end

function event(thread_id)
   execute_update(thread_id, 'y', update_blob_pad_size)
   execute_update(thread_id, 'x', update_blob_pad_size)
end
