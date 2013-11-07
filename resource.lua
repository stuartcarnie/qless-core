-------------------------------------------------------------------------------
-- Resource Class
--
-- Returns an object that represents a resource with the provided RID
-------------------------------------------------------------------------------

----
-- This gets all the data associated with the resource with the provided id. If the
-- job is not found, it returns nil. If found, it returns an object with the
-- appropriate properties
function QlessResource:data(...)
  local res = redis.call(
      'hmget', QlessResource.ns .. self.rid, 'rid', 'count')

  -- Return nil if we haven't found it
  if not res[1] then
    return nil
  end

  local data = {
    rid          = res[1],
    count        = tonumber(res[2] or 0),
    pending      = redis.call('zrevrange', self:prefix('pending'), 0, -1),
    locks        = redis.call('smembers', self:prefix('locks')),
  }

  return data
end

function QlessResource:set(count)
  count = assert(tonumber(count), 'Set(): Arg "count" not a number: ' .. tostring(count))

  redis.call('hmset', QlessResource.ns .. self.rid, 'rid', self.rid, 'count', count);

  return self.rid
end

function QlessResource:unset()
  return redis.call('del', QlessResource.ns .. self.rid);
end

function QlessResource:prefix(group)
  if group then
    return QlessResource.ns..self.rid..'-'..group
  end

  return QlessResource.ns..self.rid
end

function QlessResource:acquire(now, priority, jid)
  local keyLocks = self:prefix('locks')
  local data = self:data()
  assert(data, 'Acquire(): resource ' .. self.rid .. ' does not exist')
  assert(type(jid) ~= 'table', 'Acquire(): invalid jid')

  local remaining = data['count'] - redis.pcall('scard', keyLocks)

  if remaining > 0 then
    -- acquire a lock and release it from the pending queue
    redis.call('sadd', keyLocks, jid)
    redis.call('zrem', self:prefix('pending'), jid)
    return true
  end

  if redis.call('sismember', self:prefix('locks'), jid) == 0 then
    redis.call('zadd', self:prefix('pending'), priority - (now / 10000000000), jid)
  end

  return false
end

--- Releases the resource for the specified job identifier and assigns it to the next waiting job
-- @param now
-- @param jid
--
function QlessResource:release(jid)
  local keyLocks = self:prefix('locks')
  local keyPending = self:prefix('pending')

  redis.call('srem', keyLocks, jid)
  redis.call('zrem', keyPending, jid)

  local jids = redis.call('zrevrange', keyPending, 0, 0, 'withscores')
  if #jids == 0 then
    return false
  end

  local newJid = jids[1]
  local score = jids[2]

  redis.call('sadd', keyLocks, newJid)
  redis.call('zrem', keyPending, newJid)

  local data = Qless.job(newJid):data()
  local queue = Qless.queue(data['queue'])

  queue.work.add(score, 0, newJid)

  return newJid
end

--- Return the number of active locks for this resource
--
function QlessResource:locks()
  return redis.call('scard', self:prefix('locks'))
end

function QlessResource:exists()
  return redis.call('exists', self:prefix())
end