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

func (f *LockFactory) CreateDummyLock(key string) *Lock {
	return &Lock{
		key,
		``,
		time.Now(),
		f.RedisClient,
	}
}

func (f *LockFactory) Get(key string) *Lock {
	result, err := f.RedisClient.HGetAll(key).Result()
	if err == redis.Nil || len(result) == 0 {
		return nil
	}

	if err != nil {
		time.Sleep(time.Second * 2)

		println(`optimisticLock::Get failed: ` + err.Error(), `retrying after 2 seconds`)

		return f.Get(key)
	}

	contentStr, ok1 := result[`content`]
	updatedAt, ok2 := result[`updated_at`]
	if ok1 == false || ok2 == false {
		panic(errors.New(`Invalid value stored in Redis. "content": ` + contentStr + `, "updatedAt": ` + updatedAt))
	}

	lastUpdatedAt, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		panic(errors.New(`OptimisticLock - Internal error: updated_at is invalid time, value: ` + updatedAt))
	}

	return &Lock{
		key,
		contentStr,
		lastUpdatedAt,
		f.RedisClient,
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
		time.Sleep(time.Second * 2)

		println(`optimisticLock::Update failed: ` + err.Error(), `retrying after 2 seconds`)

		return l.Update()
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
