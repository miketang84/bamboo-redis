local hiredis = require 'hiredis'

local redis = {
    _VERSION     = 'redis-lua 3.0.0-dev',
    _DESCRIPTION = 'A Lua client library for the redis key value storage system.',
    _COPYRIGHT   = 'Copyright (C) 2009-2012 Daniele Alessandri',
}

local unpack = _G.unpack or table.unpack
local network = {}

local defaults = {
    host        = '127.0.0.1',
    port        = 6379,
    tcp_nodelay = true,
    path        = nil
}


local function parse_boolean(v)
    if v == '1' or v == 'true' or v == 'TRUE' then
        return true
    elseif v == '0' or v == 'false' or v == 'FALSE' then
        return false
    else
        return nil
    end
end

local function toboolean(value) return value == 1 end

local function sort_request(command, key, params)
    --[[ params = {
        by    = 'weight_*',
        get   = 'object_*',
        limit = { 0, 10 },
        sort  = 'desc',
        alpha = true,
    } ]]
    local query = { key }

    if params then
        if params.by then
            table.insert(query, 'BY')
            table.insert(query, params.by)
        end

        if type(params.limit) == 'table' then
            -- TODO: check for lower and upper limits
            table.insert(query, 'LIMIT')
            table.insert(query, params.limit[1])
            table.insert(query, params.limit[2])
        end

        if params.get then
            if (type(params.get) == 'table') then
                for _, getarg in pairs(params.get) do
                    table.insert(query, 'GET')
                    table.insert(query, getarg)
                end
            else
                table.insert(query, 'GET')
                table.insert(query, params.get)
            end
        end

        if params.sort then
            table.insert(query, params.sort)
        end

        if params.alpha == true then
            table.insert(query, 'ALPHA')
        end

        if params.store then
            table.insert(query, 'STORE')
            table.insert(query, params.store)
        end
    end

    return query
end


local function separate_reply(reply)
	local vals, scores = {}, {}
	for i = 1, #reply, 2 do
		table.insert(vals, reply[i])
		table.insert(scores, tonumber(reply[i + 1]))
	end

	return vals, scores
end

local function zset_range_reply(reply, withscores)
	if withscores then
		return separate_reply(reply)
	else
		return reply
	end

end


local function parse_info(response)
    local info = {}
    local current = info

    response:gsub('([^\r\n]*)\r\n', function(kv)
        if kv == '' then return end

        local section = kv:match('^# (%w+)$')
        if section then
            current = {}
            info[section:lower()] = current
            return
        end

        local k,v = kv:match(('([^:]*):([^:]*)'):rep(1))
        if k:match('db%d+') then
            current[k] = {}
            v:gsub(',', function(dbkv)
                local dbk,dbv = kv:match('([^:]*)=([^:]*)')
                current[k][dbk] = dbv
            end)
        else
            current[k] = v
        end
    end)

    return info
end

local function load_methods(proto, commands)
    local client = setmetatable ({}, getmetatable(proto))

    for cmd, fn in pairs(commands) do
        if type(fn) ~= 'function' then
            redis.error('invalid type for command ' .. cmd .. '(must be a function)')
        end
        client[cmd] = fn
    end

    for i, v in pairs(proto) do
        client[i] = v
    end

    return client
end


local default_serializer = function(cmd, ...) return ... end
local default_parser = function(reply, ...)
	if reply == hiredis.status.NIL or reply == hiredis.NIL  then
		reply = nil
	elseif reply == hiredis.status.OK or reply == hiredis.OK then
		reply = true
	elseif type(reply) == 'table' and reply.name then
		reply = reply.name
	end

	-- TODO: when meet error, hiredis return a table containing error info
	-- how to form it, use unwrap_reply may slower the performance
	return reply
end

local cmds_opts_collector = {}


local function command(cmd, opts)
    cmd, opts = string.upper(cmd), opts or {}
	if opts then
		cmds_opts_collector[cmd:lower()] = opts
	end

    if opts.handler then
        local handler = opts.handler
        return function(client, ...)
            return handler(client, cmd, ...)
        end
    end

    local serializer = opts.request or default_serializer
    local parser = opts.response or default_parser

    return function(client, ...)
 		local reply = client.conn:command(cmd, serializer(cmd, ...))
		return parser(reply, cmd, ...)
	end
end

-- ############################################################################

local client_prototype = {}

client_prototype.raw_command = function(client, ...)
--    client:write_request(table.remove(..., 1), ...)
--    return client:read_response()
end

-- Command pipelining
--[[
local ret = db:pipeline(function (p)

	for _, key in ipairs(list) do
		p:hgetall(key)
	end

end)
--]]

-- here, cmd_name is lowercase
local parse_reply = function (reply, cmd_name, ...)
  local opts = cmds_opts_collector[cmd_name]
  if opts and opts.response then
    reply = opts.response(reply, ...)
  else
    reply = default_parser(reply)
  end

  return reply
end


client_prototype.pipeline = function(client, block)
	local cmd_collector = {}
	local cmd_args_collector = {}

    local pipeline = setmetatable({}, {
        -- here, name is lowercase
        __index = function(env, name)
			-- name is command name
			return function(self, ...)
				-- collect the used commands
				table.insert(cmd_collector, name)
				table.insert(cmd_args_collector, {...})

				client.conn:append_command(name, ...)
            end
        end
    })

    local success, retval = pcall(block, pipeline)
    if not success then client.error(retval, 0) end

	local replies = {}
	for i = 1, #cmd_collector do
		table.insert(replies, client.conn:get_reply())
	end

    for i, cmd in ipairs(cmd_collector) do
      replies[i] = parse_reply(replies[i], cmd, unpack(cmd_args_collector[i]))
    end

    return replies, #cmd_collector
end

--[[
-- Publish/Subscribe
do
    local channels = function(channels)
        if type(channels) == 'string' then
            channels = { channels }
        end
        return channels
    end

    local subscribe = function(client, ...)
        client:write_request('subscribe', ...)
    end
    local psubscribe = function(client, ...)
        client:write_request('psubscribe', ...)
    end
    local unsubscribe = function(client, ...)
        client:write_request('unsubscribe')
    end
    local punsubscribe = function(client, ...)
        client:write_request('punsubscribe')
    end

    local consumer_loop = function(client)
        local aborting, subscriptions = false, 0

        local abort = function()
            if not aborting then
                unsubscribe(client)
                punsubscribe(client)
                aborting = true
            end
        end

        return coroutine.wrap(function()
            while true do
                local message
                local response = client:read_response()

                if response[1] == 'pmessage' then
                    message = {
                        kind    = response[1],
                        pattern = response[2],
                        channel = response[3],
                        payload = response[4],
                    }
                else
                    message = {
                        kind    = response[1],
                        channel = response[2],
                        payload = response[3],
                    }
                end

                if string.match(message.kind, '^p?subscribe$') then
                    subscriptions = subscriptions + 1
                end
                if string.match(message.kind, '^p?unsubscribe$') then
                    subscriptions = subscriptions - 1
                end

                if aborting and subscriptions == 0 then
                    break
                end
                coroutine.yield(message, abort)
            end
        end)
    end

    client_prototype.pubsub = function(client, subscriptions)
        if type(subscriptions) == 'table' then
            if subscriptions.subscribe then
                subscribe(client, channels(subscriptions.subscribe))
            end
            if subscriptions.psubscribe then
                psubscribe(client, channels(subscriptions.psubscribe))
            end
        end
        return consumer_loop(client)
    end
end
--]]

-- Redis transactions (MULTI/EXEC)
client_prototype.transaction = function(client, block, options)
	local cmd_collector = {}
	local cmd_args_collector = {}
	local replies = {}

	if options then
		local watch_keys = options.watch
		if type(watch_keys) == 'table' and #watch_keys > 0 then
			client.conn:command('watch', unpack(watch_keys))
		else
			client.conn:command('watch', watch_keys)
		end
	end

	local cmdi = 0
	local multi_starti = 128
	local multi_stopi = 128
    local transaction= setmetatable({}, {
        __index = function(env, name)
			-- name is command name
			return function(self, ...)
				-- collect the used commands

				if name == 'multi' then
					-- only once in each transaction
					client.conn:command(name)
					multi_starti = cmdi
				elseif name == 'exec' then
					-- only once in each transaction
					-- this is always called at last
					replies = client.conn:command(name)
					multi_stopi = cmdi
					return
				else
					-- the commands between watch and multi
					if cmdi < multi_starti or cmdi > multi_stopi then
						-- return its value immediately

						local reply = client.conn:command(name, ...)
		                return parse_reply(reply, name, ...)
					else
						-- the commands between multi and exec,
						-- we don't need its values immediately
						client.conn:command(name, ...)
						table.insert(cmd_collector, name)
						table.insert(cmd_args_collector, {...})

					end
				end
				cmdi = cmdi + 1
            end
        end
    })

    local success, retval = pcall(block, transaction)
	if not success then client.error(retval, 0) end
	if options then
      local retry_times = tonumber(options.retry)
      if type(retry_times) == 'number' and retry_times > 0 then
		-- retry body
		while success and (replies == hiredis.status.NIL or replies == hiredis.NIL) do
            cmdi = 0
            multi_starti = 128
            success = pcall(block, transaction)
            retry_times = retry_times - 1
        end
      end
	end

	if #replies > 0 then
        for i, cmd in ipairs(cmd_collector) do
            replies[i] = parse_reply(replies[i], cmd, unpack(cmd_args_collector[i]))
        end
	end

    return replies, #cmd_collector
end


--[[
-- MONITOR context

do
    local monitor_loop = function(client)
        local monitoring = true

        -- Tricky since the payload format changed starting from Redis 2.6.
        local pattern = '^(%d+%.%d+)( ?.- ?) ?"(%a+)" ?(.-)$'

        local abort = function()
            monitoring = false
        end

        return coroutine.wrap(function()
            client:monitor()

            while monitoring do
                local message, matched
                local response = client:read_response()

                local ok = response:gsub(pattern, function(time, info, cmd, args)
                    message = {
                        timestamp = tonumber(time),
                        client    = info:match('%d+.%d+.%d+.%d+:%d+'),
                        database  = tonumber(info:match('%d+')) or 0,
                        command   = cmd,
                        arguments = args:match('.+'),
                    }
                    matched = true
                end)

                if not matched then
                    client.error('Unable to match MONITOR payload: '..response)
                end

                coroutine.yield(message, abort)
            end
        end)
    end

    client_prototype.monitor_messages = function(client)
        return monitor_loop(client)
    end
end
--]]


redis.command = command

function redis.error(message, level)
    error(message, (level or 1) + 1)
end


function redis.connect(host, port)
	host = host or '127.0.0.1'
	port = port or 6379
	local conn = assert(hiredis.connect(host, port))

    local commands = redis.commands or {}
    if type(commands) ~= 'table' then
        redis.error('invalid type for the commands table')
    end

    local client = load_methods(client_prototype, commands)
    client.error = redis.error
    client.conn = conn

    return client

end

-- ############################################################################

-- Commands defined in this table do not take the precedence over
-- methods defined in the client prototype table.

redis.commands = {
    -- commands operating on the key space
    exists           = command('EXISTS', {
        response = toboolean
    }),
    del              = command('DEL'),
    type             = command('TYPE'),
    rename           = command('RENAME'),
    renamenx         = command('RENAMENX', {
        response = toboolean
    }),
    expire           = command('EXPIRE', {
        response = toboolean
    }),
    pexpire          = command('PEXPIRE', {     -- >= 2.6
        response = toboolean
    }),
    expireat         = command('EXPIREAT', {
        response = toboolean
    }),
    pexpireat        = command('PEXPIREAT', {   -- >= 2.6
        response = toboolean
    }),
    ttl              = command('TTL'),
    pttl             = command('PTTL'),         -- >= 2.6
    move             = command('MOVE', {
        response = toboolean
    }),
    dbsize           = command('DBSIZE'),
    persist          = command('PERSIST', {     -- >= 2.2
        response = toboolean
    }),
    keys             = command('KEYS'),
    randomkey        = command('RANDOMKEY', {
        response = function(response)
            if response == '' then
                return nil
            else
                return response
            end
        end
    }),
    sort             = command('SORT', {
        request = sort_request,
    }),

    -- commands operating on string values
    set              = command('SET'),
    setnx            = command('SETNX', {
        response = toboolean
    }),
    setex            = command('SETEX'),        -- >= 2.0
    psetex           = command('PSETEX'),       -- >= 2.6
    mset             = command('MSET', {
--        request = mset_request
    }),
    msetnx           = command('MSETNX', {
--        request  = mset_request,
        response = toboolean
    }),
    get              = command('GET'),
    mget             = command('MGET'),
    getset           = command('GETSET'),
    incr             = command('INCR'),
    incrby           = command('INCRBY'),
    incrbyfloat      = command('INCRBYFLOAT', { -- >= 2.6
        response = function(reply, command, ...)
            return tonumber(reply)
        end,
    }),
    decr             = command('DECR'),
    decrby           = command('DECRBY'),
    append           = command('APPEND'),       -- >= 2.0
    substr           = command('SUBSTR'),       -- >= 2.0
    strlen           = command('STRLEN'),       -- >= 2.2
    setrange         = command('SETRANGE'),     -- >= 2.2
    getrange         = command('GETRANGE'),     -- >= 2.2
    setbit           = command('SETBIT'),       -- >= 2.2
    getbit           = command('GETBIT'),       -- >= 2.2

    -- commands operating on lists
    rpush            = command('RPUSH'),
    lpush            = command('LPUSH'),
    llen             = command('LLEN'),
    lrange           = command('LRANGE'),
    ltrim            = command('LTRIM'),
    lindex           = command('LINDEX'),
    lset             = command('LSET'),
    lrem             = command('LREM'),
    lpop             = command('LPOP'),
    rpop             = command('RPOP'),
    rpoplpush        = command('RPOPLPUSH'),
    blpop            = command('BLPOP'),        -- >= 2.0
    brpop            = command('BRPOP'),        -- >= 2.0
    rpushx           = command('RPUSHX'),       -- >= 2.2
    lpushx           = command('LPUSHX'),       -- >= 2.2
    linsert          = command('LINSERT'),      -- >= 2.2
    brpoplpush       = command('BRPOPLPUSH'),   -- >= 2.2

    -- commands operating on sets
    sadd             = command('SADD'),
    srem             = command('SREM'),
    spop             = command('SPOP'),
    smove            = command('SMOVE', {
        response = toboolean
    }),
    scard            = command('SCARD'),
    sismember        = command('SISMEMBER', {
        response = toboolean
    }),
    sinter           = command('SINTER'),
    sinterstore      = command('SINTERSTORE'),
    sunion           = command('SUNION'),
    sunionstore      = command('SUNIONSTORE'),
    sdiff            = command('SDIFF'),
    sdiffstore       = command('SDIFFSTORE'),
    smembers         = command('SMEMBERS'),
    srandmember      = command('SRANDMEMBER'),

    -- commands operating on sorted sets
    zadd             = command('ZADD'),
    zincrby          = command('ZINCRBY'),
    zrem             = command('ZREM'),
    zrange           = command('ZRANGE', {
		response = function (reply, command, key, start, stop, withscores)
			return zset_range_reply(reply, withscores)
		end,
	}),
    zrevrange        = command('ZREVRANGE', {
		response = function (reply, command, key, start, stop, withscores)
			return zset_range_reply(reply, withscores)
		end,
	}),
    zrangebyscore    = command('ZRANGEBYSCORE', {
		response = function (reply, command, key, min, max, withscores)
			return zset_range_reply(reply, withscores)
		end,
	}),
    zrevrangebyscore = command('ZREVRANGEBYSCORE', {
		response = function (reply, command, key, min, max, withscores)
			return zset_range_reply(reply, withscores)
		end,
	}),
    zunionstore      = command('ZUNIONSTORE', {         -- >= 2.0
--        request = zset_store_request
    }),
    zinterstore      = command('ZINTERSTORE', {         -- >= 2.0
--        request = zset_store_request
    }),
    zcount           = command('ZCOUNT'),
    zcard            = command('ZCARD'),
    zscore           = command('ZSCORE', {
      response = function (reply, command, key, val)
        local nreply = tonumber(reply)
        if type(nreply) == 'number' then
          return nreply
        else
          return nil
        end
      end
    }),
    zremrangebyscore = command('ZREMRANGEBYSCORE'),
    zrank            = command('ZRANK'),                -- >= 2.0
    zrevrank         = command('ZREVRANK'),             -- >= 2.0
    zremrangebyrank  = command('ZREMRANGEBYRANK'),      -- >= 2.0

    -- commands operating on hashes
    hset             = command('HSET', {        -- >= 2.0
        response = toboolean
    }),
    hsetnx           = command('HSETNX', {      -- >= 2.0
        response = toboolean
    }),
    hmset            = command('HMSET', {       -- >= 2.0
--        request  = function (command, ...)
--        end
    }),
    hincrby          = command('HINCRBY'),      -- >= 2.0
    hincrbyfloat     = command('HINCRBYFLOAT', {-- >= 2.6
        response = function(reply, command, ...)
            return tonumber(reply)
        end,
    }),
    hget             = command('HGET'),         -- >= 2.0
    hmget            = command('HMGET', {       -- >= 2.0
    }),
    hdel             = command('HDEL'),        -- >= 2.0
    hexists          = command('HEXISTS', {     -- >= 2.0
        response = toboolean
    }),
    hlen             = command('HLEN'),         -- >= 2.0
    hkeys            = command('HKEYS'),        -- >= 2.0
    hvals            = command('HVALS'),        -- >= 2.0
    hgetall          = command('HGETALL', {     -- >= 2.0
        response = function(reply, command, ...)
            local new_reply = {}
            for i = 1, #reply, 2 do new_reply[reply[i]] = reply[i + 1] end
            return new_reply
        end
    }),

    -- connection related commands
    ping             = command('PING', {
        response = function(response) return response == 'PONG' end
    }),
    echo             = command('ECHO'),
    auth             = command('AUTH'),
    select           = command('SELECT'),
    quit             = command('QUIT'),

    -- transactions
    multi            = command('MULTI'),        -- >= 2.0
    exec             = command('EXEC'),         -- >= 2.0
    discard          = command('DISCARD'),      -- >= 2.0
    watch            = command('WATCH'),        -- >= 2.2
    unwatch          = command('UNWATCH'),      -- >= 2.2

    -- publish - subscribe
    subscribe        = command('SUBSCRIBE'),    -- >= 2.0
    unsubscribe      = command('UNSUBSCRIBE'),  -- >= 2.0
    psubscribe       = command('PSUBSCRIBE'),   -- >= 2.0
    punsubscribe     = command('PUNSUBSCRIBE'), -- >= 2.0
    publish          = command('PUBLISH'),      -- >= 2.0

    -- redis scripting
    eval             = command('EVAL'),         -- >= 2.6
    evalsha          = command('EVALSHA'),      -- >= 2.6
    script           = command('SCRIPT'),       -- >= 2.6

    -- remote server control commands
    bgrewriteaof     = command('BGREWRITEAOF'),
    config           = command('CONFIG', {     -- >= 2.0
        response = function(reply, command, ...)
            if (type(reply) == 'table') then
                local new_reply = { }
                for i = 1, #reply, 2 do new_reply[reply[i]] = reply[i + 1] end
                return new_reply
            end

            return reply
        end
    }),
    client           = command('CLIENT'),       -- >= 2.4
    slaveof          = command('SLAVEOF'),
    save             = command('SAVE'),
    bgsave           = command('BGSAVE'),
    lastsave         = command('LASTSAVE'),
    flushdb          = command('FLUSHDB'),
    flushall         = command('FLUSHALL'),
    monitor          = command('MONITOR'),
    time             = command('TIME'),         -- >= 2.6
    slowlog          = command('SLOWLOG', {     -- >= 2.2.13
        response = function(reply, command, ...)
            if (type(reply) == 'table') then
                local structured = { }
                for index, entry in ipairs(reply) do
                    structured[index] = {
                        id = tonumber(entry[1]),
                        timestamp = tonumber(entry[2]),
                        duration = tonumber(entry[3]),
                        command = entry[4],
                    }
                end
                return structured
            end

            return reply
        end
    }),
    info             = command('INFO', {
        response = parse_info,
    }),
    shutdown         = command('SHUTDOWN'),
}

-- ############################################################################

return redis
