-------------------------------------------------------------------------------
-- Configuration interactions
-------------------------------------------------------------------------------

-- This represents our default configuration settings
Qless.config.defaults = {
    ['application']        = 'qless',
    ['heartbeat']          = 60,
    ['stats-history']      = 30,
    ['histogram-history']  = 7,
    ['jobs-history-count'] = 50000,
    ['jobs-history']       = 604800
}

-- Get one or more of the keys
Qless.config.get = function(key, default)
    if key then
        return redis.call('hget', 'ql:config', key) or
            Qless.config.defaults[key] or default
    else
        -- Inspired by redis-lua https://github.com/nrk/redis-lua/blob/version-2.0/src/redis.lua
        local reply = redis.call('hgetall', 'ql:config')
        for i = 1, #reply, 2 do
            Qless.config.defaults[reply[i]] = reply[i + 1]
        end
        return Qless.config.defaults
    end
end

-- Set a configuration variable
Qless.config.set = function(option, value)
    assert(option, 'config.set(): Arg "option" missing')
    assert(value , 'config.set(): Arg "value" missing')
    -- Send out a log message
    redis.call('publish', 'ql:log', cjson.encode({
        event  = 'config.set',
        option = option
    }))

    redis.call('hset', 'ql:config', option, value)
end

-- Unset a configuration option
Qless.config.unset = function(option)
    assert(option, 'config.unset(): Arg "option" missing')
    -- Send out a log message
    redis.call('publish', 'ql:log', cjson.encode({
        event  = 'config.unset',
        option = option
    }))

    redis.call('hdel', 'ql:config', option)
end