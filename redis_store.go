package redisfarm

import (
	"errors"
	"strconv"
	"strings"

	"github.com/mediocregopher/radix/v3"
	"github.com/rs/zerolog"
)

const RedisPUBSUBPrefix string = "CMND"

// RedisStore wrapps redis client implementation and provides high-level API
// to deal with redis storage.
//
// It's important: redis database is used as intraday data storage.
// Database cleans automatically by another side every next day.
type RedisStore struct {
	pool       *radix.Pool
	pubsub     radix.PubSubConn
	connection string
	db         int
	qprefix    string
	log        zerolog.Logger
}

var _ Storer = (*RedisStore)(nil)

// NewRedisStore returns instance to RedisStore.
func NewRedisStore(connection string, db int, zl *zerolog.Logger) *RedisStore {
	res := RedisStore{
		connection: connection,
		db:         db,
		qprefix:    RedisPUBSUBPrefix,
		log:        zl.With().Str("layer", "store").Str("connection", connection).Int("db", db).Logger(),
	}

	return &res
}

func (rs *RedisStore) Connect() error {
	var err error

	var customConnFunc = func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, radix.DialSelectDB(rs.db))
	}
	rs.pool, err = radix.NewPool("tcp", rs.connection, 10, radix.PoolConnFunc(customConnFunc))
	if err != nil {
		return err
	}

	rs.pubsub, err = radix.PersistentPubSubWithOpts("tcp", rs.connection, radix.PersistentPubSubConnFunc(customConnFunc))
	if err != nil {
		return err
	}

	return nil
}

func (rs *RedisStore) Close() {
	if rs.pool != nil {
		rs.pool.Close()
	}
	rs.pubsub.Close()
	return
}

func (rs *RedisStore) Subscribe(tag string, dst chan ChannelMessage, channels ...string) error {

	redisch := make(chan radix.PubSubMessage)

	err := rs.pubsub.Subscribe(redisch, channels...)
	if err != nil {
		return err
	}

	go func(t string, c chan radix.PubSubMessage) {
		for msg := range c {
			dst <- ChannelMessage{Tag: t, DB: rs.db, PubSubMessage: msg}
		}
	}(tag, redisch)

	return nil
}

// Scan returns all instance names startings with "pattern".
func (rs *RedisStore) Scan(pattern string, result *[]string) error {
	*result = (*result)[0:0]

	s := radix.NewScanner(rs.pool, radix.ScanOpts{Command: "SCAN", Pattern: pattern})
	var key string
	for s.Next(&key) {
		*result = append(*result, key)
	}

	return s.Close()
}

//
func (rs *RedisStore) Keys(pattern string, result *[]string) error {
	return rs.pool.Do(radix.Cmd(result, "KEYS", pattern))
}

func (rs *RedisStore) GetObject(key string, result interface{}) error {
	return rs.pool.Do(radix.Cmd(result, "HGETALL", key))
}

func (rs *RedisStore) Get(key string, val interface{}) error {
	return rs.pool.Do(radix.Cmd(val, "GET", key))
}

func (rs *RedisStore) MGet(keys []string, val *[]string) error {
	return rs.pool.Do(radix.Cmd(val, "MGET", keys...))
}

func (rs *RedisStore) Set(key string, val string) error {
	err := rs.pool.Do(radix.Cmd(nil, "SET", key, val))
	if err != nil {
		return errors.New("redis key " + key + " setting failed. Detais: " + err.Error())
	}
	return err
}

func (rs *RedisStore) Publish(code string) (bool, error) {

	var res int
	err := rs.pool.Do(radix.Cmd(&res, "PUBLISH", rs.qprefix+":"+strconv.Itoa(rs.db), code))
	if err != nil {
		return false, errors.New("redis publish " + code + " to channel " + rs.qprefix + " failed. Detais: " + err.Error())
	}
	return res > 0, err
}

func (rs *RedisStore) PublishEx(channel string, msg string) (bool, error) {

	var res int
	err := rs.pool.Do(radix.Cmd(&res, "PUBLISH", channel, msg))
	if err != nil {
		return false, errors.New("redis publish " + msg + " to channel " + channel + " failed. Detais: " + err.Error())
	}

	if res > 0 {
		rs.log.Debug().Str("channel", channel).Str("msg", msg).Msg("message sent succesfully")
	}
	return res > 0, err
}

func (rs *RedisStore) DB() int {
	return rs.db
}

func (rs *RedisStore) ListRange(key string, from, to int) ([]string, error) {

	var res []string
	err := rs.pool.Do(radix.Cmd(&res, "LRANGE", key, strconv.Itoa(from), strconv.Itoa(to)))
	if err != nil {
		return nil, errors.New("redis lrange " + key + " failed. Detais: " + err.Error())
	}

	return res, nil
}

func (rs *RedisStore) ListLen(key string) (int, error) {

	keyb := []byte(key)

	if strings.HasPrefix(key, "M:") {
		keyb[0] = 'L'
		key = string(keyb)
	} else if strings.HasPrefix(key, "L:") == false {
		key = "L:" + key
	}

	var res int
	err := rs.pool.Do(radix.Cmd(&res, "LLEN", key))
	if err != nil {
		return 0, errors.New("redis lrange " + key + " failed. Detais: " + err.Error())
	}

	return res, nil
}

func (rs *RedisStore) LLen(key string) (int, error) {

	var res int
	err := rs.pool.Do(radix.Cmd(&res, "LLEN", key))
	if err != nil {
		return 0, errors.New("redis lrange " + key + " failed. Detais: " + err.Error())
	}

	return res, nil
}

func (rs *RedisStore) HGet(key, field string) (string, error) {
	var res string
	err := rs.pool.Do(radix.Cmd(&res, "HGET", key, field))
	if err != nil {
		return "", errors.New("redis HGET " + key + " getting failed. Detais: " + err.Error())
	}
	return res, err
}

func (rs *RedisStore) Lpush(key string, element string) error {
	var res int

	err := rs.pool.Do(radix.Cmd(&res, "LPUSH", key, element))
	if err != nil {
		return errors.New("redis LPUSH  " + element + " to  " + key + " failed. Detais: " + err.Error())
	}
	return nil
}

func (rs *RedisStore) Rpush(key string, element string) error {
	var res int

	err := rs.pool.Do(radix.Cmd(&res, "RPUSH", key, element))
	if err != nil {
		return errors.New("redis RPUSH  " + element + " to  " + key + " failed. Detais: " + err.Error())
	}
	return nil
}

func (rs *RedisStore) Lrem(key, element string, cnt int) error {
	var res int

	err := rs.pool.Do(radix.Cmd(&res, "LREM", key, strconv.Itoa(cnt), element))
	if err != nil {
		return errors.New("redis LREM  " + element + " to  " + key + " failed. Detais: " + err.Error())
	}
	return nil
}

func (rs *RedisStore) Do(cmd string, args ...string) error {
	var res int

	err := rs.pool.Do(radix.Cmd(&res, cmd, args...))
	if err != nil {
		return errors.New("redis " + cmd + " failed. Detais: " + err.Error())
	}
	return nil
}
