package redisfarm

import "github.com/mediocregopher/radix/v3"

// Storer is and interface definition to data persistent storage.
type Storer interface {
	Scan(pattern string, result *[]string) error
	Keys(pattern string, result *[]string) error
	Get(key string, val interface{}) error
	MGet(keys []string, result *[]string) error
	Set(key string, val string) error
	Publish(code string) (bool, error)
	PublishEx(chanel string, msg string) (bool, error)
	ListLen(key string) (int, error)
	LLen(key string) (int, error)
	ListRange(key string, from, to int) ([]string, error)
	GetObject(key string, result interface{}) error
	HGet(key, field string) (string, error)
	DB() int
	Close()
	Subscribe(tag string, dst chan ChannelMessage, channels ...string) error
	Lpush(key string, element string) error
	Rpush(key string, element string) error
	Do(cmd string, args ...string) error
	Lrem(key, element string, cnt int) error
	Connect() error
	Exists(key string) (bool, error)
}

var _ Storer = (*RedisStore)(nil)

//
type StoreFarmer interface {
	Add(code string, s Storer)
	ByCode(code string) Storer
	ByDB(db int) Storer
	Codes() []string
}

type ChannelMessage struct {
	Tag string
	DB  int
	radix.PubSubMessage
}
