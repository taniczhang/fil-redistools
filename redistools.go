package redistools

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type RedisDataStore struct {
	RedisHost string
	RedisDB   string
	RedisPwd  string
	Timeout   int64
	RedisPool *redis.Pool
}

func (r *RedisDataStore) NewPool() *redis.Pool {

	return &redis.Pool{
		Dial:        r.RedisConnect,
		MaxIdle:     10,
		MaxActive:   0,
		IdleTimeout: 1 * time.Second,
		Wait:        true,
	}
}

func (r *RedisDataStore) RedisConnect() (redis.Conn, error) {
	c, err := redis.Dial("tcp", r.RedisHost)
	if err != nil {
		return nil, err
	}
	_, err = c.Do("AUTH", r.RedisPwd)

	if err != nil {
		return nil, err
	}

	_, err = c.Do("SELECT", r.RedisDB)
	if err != nil {
		return nil, err
	}

	redis.DialConnectTimeout(time.Duration(r.Timeout) * time.Second)
	redis.DialReadTimeout(time.Duration(r.Timeout) * time.Second)
	redis.DialWriteTimeout(time.Duration(r.Timeout) * time.Second)

	return c, nil
}

func (r *RedisDataStore) Get(k string) (interface{}, error) {
	c := r.RedisPool.Get()
	defer c.Close()
	v, err := c.Do("GET", k)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (r *RedisDataStore) Set(k, v string) error {
	c := r.RedisPool.Get()
	defer c.Close()
	_, err := c.Do("SET", k, v)
	return err
}

func (r *RedisDataStore) SetEx(k string, v interface{}, ex int64) error {
	c := r.RedisPool.Get()
	defer c.Close()
	_, err := c.Do("SET", k, v, "EX", ex)
	return err
}