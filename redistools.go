package redistools

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

var (
	DEFAULT = time.Duration(0)  // 过期时间 不设置
	FOREVER = time.Duration(-1) // 过期时间不设置
)

type Cache struct {
	pool              *redis.Pool
	defaultExpiration time.Duration
}

// 返回cache 对象, 在多个工具之间建立一个 中间初始化的时候使用
func NewRedisCache(db int, host string, defaultExpiration time.Duration) Cache {
	pool := &redis.Pool{
		MaxActive:   10,                              //  最大连接数，即最多的tcp连接数，一般建议往大的配置，但不要超过操作系统文件句柄个数（centos下可以ulimit -n查看）
		MaxIdle:     10,                              // 最大空闲连接数，即会有这么多个连接提前等待着，但过了超时时间也会关闭。
		IdleTimeout: time.Duration(100) * time.Second, // 空闲连接超时时间，但应该设置比redis服务器超时时间短。否则服务端超时了，客户端保持着连接也没用
		Wait:        true,                             // 当超过最大连接数 是报错还是等待， true 等待 false 报错
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", host, redis.DialDatabase(db))
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return conn, nil
		},
	}
	return Cache{pool: pool, defaultExpiration: defaultExpiration}
}


// string 类型 添加, v 可以是任意类型
func (c Cache) StringSet(name string, v []byte) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", name, v)
	return err
}

// 获取 字符串类型的值
func (c Cache) StringGet(name string) ([]byte,error) {
	conn := c.pool.Get()
	defer conn.Close()
	temp, err := redis.Bytes(conn.Do("Get", name))
	return temp,err
}


// 判断所在的 key 是否存在
func (c Cache) Exist(name string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Bool(conn.Do("EXISTS", name))
	return v, err
}

// 自增
func (c Cache) StringIncr(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("INCR", name))
	return v, err
}

// 设置过期时间 （单位 秒）
func (c Cache) Expire(name string, newSecondsLifeTime int64) error {
	// 设置key 的过期时间
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("EXPIRE", name, newSecondsLifeTime)
	return err
}

// 删除指定的键
func (c Cache) Delete(keys ...interface{}) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Bool(conn.Do("DEL", keys...))
	return v, err
}

// 查看指定的长度
func (c Cache) StrLen(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("STRLEN", name))
	return v, err
}

// //////////////////  hash ///////////
// 删除指定的 hash 键
func (c Cache) Hdel(name, key string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	var err error
	v, err := redis.Bool(conn.Do("HDEL", name, key))
	return v, err
}

// 查看hash 中指定是否存在
func (c Cache) HExists(name, field string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	var err error
	v, err := redis.Bool(conn.Do("HEXISTS", name, field))
	return v, err
}

// 获取hash 的键的个数
func (c Cache) HLen(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("HLEN", name))
	return v, err
}

// 传入的 字段列表获得对应的值
func (c Cache) HMget(name string, fields ...string) ([]interface {}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	args := []interface{}{name}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := redis.Values(conn.Do("HMGET", args...))

	return value, err
}

// 设置单个值, value 还可以是一个 map slice 等
func (c Cache) HSet(name string, key string, v[]byte) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	_, err = conn.Do("HSET", name, key, v)
	return
}

// 设置多个值 , obj 可以是指针 slice map struct
func (c Cache) HMSet(name string, obj interface{}) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	_, err = conn.Do("HMSET", redis.Args{}.Add(name).AddFlat(&obj)...)
	return
}

// 获取单个hash 中的值
func (c Cache) HGet(name, field string) (value []byte,err error) {
	conn := c.pool.Get()
	defer conn.Close()
	temp, err := redis.Bytes(conn.Do("HGET", name, field))
	return temp,err
}

// 获取 set 集合中所有的元素, 想要什么类型的自己指定
func (c Cache) Smembers(name string) (byt[]byte,err error) {
	conn := c.pool.Get()
	defer conn.Close()
	temp, err := redis.Bytes(conn.Do("SMEMBERS", name))
	return temp,err
}

// 获取集合中元素的个数
func (c Cache) ScardInt64s(name string) (int64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int64(conn.Do("SCARD", name))
	return v, err
}
