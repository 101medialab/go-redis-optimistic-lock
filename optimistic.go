package optimisticLock

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
)

func New(r *redis.Client) *LockFactory {
	return &LockFactory{RedisClient: r}
}

type LockFactory struct {
	RedisClient *redis.Client `inject:""`
}

func (h *LockFactory) Get(key string) *Lock {
	result, err := h.RedisClient.HGetAll(key).Result()
	if err == redis.Nil || len(result) == 0 {
		return nil
	}

	if err != nil {
		// Unknown error
		panic(err)
	}

	contentStr, ok1 := result[`content`]
	updatedAt, ok2 := result[`updated_at`]
	if ok1 == false || ok2 == false {
		panic(errors.New(`The value in redis mismatch. `))
	}

	lastUpdatedAt, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		panic(errors.New(`OptimisticLock - Internal error: updated_at is invalid time. `))
	}

	return &Lock{
		key,
		contentStr,
		lastUpdatedAt,
		h.RedisClient,
	}
}

type Lock struct {
	Key           string
	Content       string
	lastUpdatedAt time.Time
	redisClient   *redis.Client
}

func (l *Lock) Update() bool {
	// FIXME: use evalsha instead of direct scripting
	result, err := l.redisClient.Eval(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	if redis.call('HGET', KEYS[1], 'updated_at') ~= KEYS[3] then
		return 0
	end 
end 
redis.call('HMSET', KEYS[1], 'content', KEYS[2], 'updated_at', KEYS[4]) 
return 1
`,
		[]string{
			l.Key,
			l.Content,
			l.lastUpdatedAt.Format(time.RFC3339Nano),
			time.Now().UTC().Format(time.RFC3339Nano),
		},
	).Result()

	if err != nil {
		panic(err)
	}

	switch t := result.(type) {
	case string:
		return t == `1`

	case int64:
		return t == 1

	default:
		// FIXME: should not reach this line
		panic(errors.New(`Unknown return from redis eval. `))
	}

	return false
}
