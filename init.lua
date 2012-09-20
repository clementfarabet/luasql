----------------------------------------------------------------------
--
-- Copyright (c) 2012 Clement Farabet
-- 
-- Permission is hereby granted, free of charge, to any person obtaining
-- a copy of this software and associated documentation files (the
-- "Software"), to deal in the Software without restriction, including
-- without limitation the rights to use, copy, modify, merge, publish,
-- distribute, sublicense, and/or sell copies of the Software, and to
-- permit persons to whom the Software is furnished to do so, subject to
-- the following conditions:
-- 
-- The above copyright notice and this permission notice shall be
-- included in all copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
-- EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
-- MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
-- NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
-- LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
-- OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
-- WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
--
----------------------------------------------------------------------
-- description:
--     luasql - a simple table-oriented interface to SQL-like databases
--              It builds on LuaSQL 2.1 (http://www.keplerproject.org/luasql/),
--              which provides a raw interface to SQL dbs.
----------------------------------------------------------------------

-- library:
luasql = {}

-- private functions (belong to db object):
local function create(db, args)
   -- parse args
   local tbl = args.table
   local columns = args.columns -- a table of {name,type} entries

   -- table -> string
   local declarations = {}
   for i,entry in ipairs(columns) do
      local fentry = table.concat(entry, ' ')
      table.insert(declarations, fentry)
   end
   declarations = table.concat(declarations, ',')
   
   -- build cmd
   local cmd = string.format("CREATE TABLE %s(%s)", tbl, declarations)

   -- execute cmd
   local res,err = db.conn:execute(cmd)
   if err then print(err)
   else print('LuaSQL: created table ' .. tbl) end
   return res
end

local function drop(db, args)
   -- parse args
   local tbl = args.table

   -- cmd
   local cmd = string.format("DROP TABLE %s", tbl)

   -- execute cmd
   local res,err = db.conn:execute(cmd)
   if err then print(err)
   else print('LuaSQL: dropped table ' .. tbl) end
   return res
end

local function insert(db, args)
   -- parse args
   local tbl = args.table
   local entries = args.entries

   -- one cmd / entry
   local res,err
   for i,entry in ipairs(entries) do
      -- flatten entry
      local fentry = {}
      for k,v in pairs(entry) do
         table.insert(fentry, k .. ' = "' .. v .. '"')
      end
      fentry = table.concat(fentry, ', ')

      -- build cmd
      local cmd = string.format("INSERT INTO %s SET %s", tbl, fentry)

      -- exec cmd
      res,err = db.conn:execute(cmd)
      if err then break end
   end

   -- error?
   if err then print(err)
   else print('LuaSQL: inserted ' .. #entries .. ' entries into table ' .. tbl) end
   return res
end

local function select(db, args)
   -- parse args
   local tbl = args.table
   local columns = args.columns or {'*'}
   local query = args.query

   -- make cmd
   local cmd = string.format('SELECT %s FROM %s', table.concat(columns,', '), tbl)
   if query then
      cmd = cmd .. ' WHERE ' .. query
   end

   -- exec cmd
   local entries = {}
   res,err = db.conn:execute(cmd)
   if err then print(err)
   else
      local row = res:fetch({},'a') table.insert(entries, row)
      while row do
         row = res:fetch({},'a') table.insert(entries, row)
      end
      print('LuaSQL: retrieved '.. #entries ..' entries from table ' .. tbl)
   end

   -- done
   return entries
end

-- public function:
function luasql.connect(args)
   -- parse args
   args = args or {}
   local type = args.type or 'mysql'
   local database = args.database or 'test'
   local hostname = args.host or 'localhost'
   local user = args.user or user
   local password = args.password or password

   -- create new env
   local env = require('luasql.'..type)[type]()

   -- connect to DB
   local conn = assert(env:connect(database, user, password, hostname))

   -- return db object
   local db = {
      conn = conn,
      create = create,
      drop = drop,
      insert = insert,
      select = select
   }
   return db
end
