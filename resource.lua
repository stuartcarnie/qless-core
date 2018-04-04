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
      'hmget', QlessResource.ns .. self.rid, 'rid', 'max')

  -- Return nil if we haven't found it
  if not res[1] then
    return nil
  end

  local data = {
    rid          = res[1],
    max          = tonumber(res[2] or 0),
    pending      = redis.call('zrevrange', self:prefix('pending'), 0, -1),
    locks        = redis.call('smembers', self:prefix('locks')),
  }

  return data
end

---
-- Stats oriented call to view the counts of a single resource with the
-- provided name or all resource stats.  If a single or all resource are
-- not found, it returns nil.
-- @param now
-- @param name
--
function QlessResource:counts(now, rid)
  if rid then
    local resource = redis.call(
      'hmget', QlessResource.ns .. rid, 'rid', 'max')

    -- Return nil if we haven't found it
    if not resource[1] then
      return nil
    end

    local pending = redis.call('zrevrange', QlessResource.ns .. rid .. '-' .. 'pending', 0, -1)
    local pcount = 0
    for _, _ in pairs(pending) do
      pcount = pcount + 1
    end

    local locks = redis.call('smembers', QlessResource.ns .. rid .. '-' .. 'locks')
    local lcount = 0
    for _, _ in pairs(locks) do
      lcount = lcount + 1
    end

    return {
      rid          = resource[1],
      max          = tonumber(resource[2] or 0),
      pending      = tonumber(pcount or 0),
      locks        = tonumber(lcount or 0)
    }
  else
    local resources = redis.call('smembers', 'ql:resources')
    local response = {}
    for _, rname in ipairs(resources) do
      local c = QlessResource:counts(now, rname)
      response[rname] = {
        max        = c.max,
        pending    = c.pending,
        locks      = c.locks
      }
    end

    return response
  end
end

function QlessResource:set(max)
  local max = assert(tonumber(max), 'Set(): Arg "max" not a number: ' .. tostring(max))

  redis.call('sadd', 'ql:resources', self.rid)
  redis.call('hmset', QlessResource.ns .. self.rid, 'rid', self.rid, 'max', max);

  return self.rid
end

function QlessResource:unset()
  redis.call('srem', 'ql:resources', self.rid)
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

  -- Don't allow multiple locks on same aquire
  if redis.call('sismember', self:prefix('locks'), jid) == 1 then
    return true
  end

  local remaining = data['max'] - redis.pcall('scard', keyLocks)

  if remaining > 0 then
    -- acquire a lock and release it from the pending queue
    redis.call('sadd', keyLocks, jid)
    redis.call('zrem', self:prefix('pending'), jid)
    return true
  end

  local pending = redis.call('zscore', self:prefix('pending'), jid)
  if pending == nil or pending == false then
    redis.call('zadd', self:prefix('pending'), priority - (now / 10000000000), jid)
  end

  return false
end

--- Releases the resource for the specified job identifier and assigns it to the next waiting job
-- @param now
-- @param jid
--
function QlessResource:release(now, jid)
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

  -- multiple resource validation
  if Qless.job(newJid):acquire_resources(now) then
    local data = Qless.job(newJid):data()
    Qless.queue(data['queue']).work.add(score, 0, newJid)
  end

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