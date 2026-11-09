local cap = tonumber(ARGV[1]); local window = tonumber(ARGV[2])
local t = redis.call('TIME'); local now = t[1]*1000 + math.floor(t[2]/1000)
local rate = cap / window                                  -- token/ms
local s = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(s[1]) or cap; local ts = tonumber(s[2]) or now
tokens = math.min(cap, tokens + (now - ts) * rate)
local allowed = 0; local retry = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 else retry = math.ceil((1 - tokens) / rate) end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], window)
return {allowed, math.floor(tokens), retry}
