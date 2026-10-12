-- 设备上线 Outbox 幂等入队（EDD-VAGW-DSN-CR-012 §3.5）
-- KEYS[1] = record hash key（去重标记 + 发布状态）
-- KEYS[2] = stream key
-- KEYS[3] = online session zset key
-- ARGV[1] = eventId
-- ARGV[2] = kafkaKey (hsmUid)
-- ARGV[3] = payload json
-- ARGV[4] = aggregateKeyHash
-- ARGV[5] = occurredAt epoch millis
-- ARGV[6] = record ttl seconds
-- ARGV[7] = hsmUid（online member）
-- ARGV[8] = online score epoch millis
-- ARGV[9] = stream maxlen
if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('ZADD', KEYS[3], ARGV[8], ARGV[7])
    return 'DUPLICATE'
end
redis.call('HSET', KEYS[1],
    'eventId', ARGV[1],
    'kafkaKey', ARGV[2],
    'aggregateKeyHash', ARGV[4],
    'occurredAt', ARGV[5],
    'status', 'PENDING',
    'attempts', '0')
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[9], '*',
    'eventId', ARGV[1],
    'kafkaKey', ARGV[2],
    'payload', ARGV[3],
    'aggregateKeyHash', ARGV[4],
    'occurredAt', ARGV[5])
redis.call('ZADD', KEYS[3], ARGV[8], ARGV[7])
return 'ENQUEUED'
