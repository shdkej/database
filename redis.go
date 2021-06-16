package database

import (
	"log"
	"os"

	"github.com/go-redis/redis"
)

type Redis struct {
	redis *redis.Client
}

func (c *Redis) Init() error {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	c.redis = redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: "",
		DB:       0,
	})

	c.Ping()

	return nil
}

func (c *Redis) Ping() error {
	pong, err := c.redis.Ping().Result()
	if err != nil {
		return err
	}

	log.Println(pong, err)
	return nil
}

func (c *Redis) Hits(page string) (int64, error) {
	hitstring := page + ":hits"
	hits, err := c.redis.Incr(hitstring).Result()
	if err != nil {
		log.Println("hutswing")
		return 0, err
	}
	return hits, nil
}

func (c *Redis) Create(tag map[string]interface{}) error {
	err := c.redis.HMSet(tag["Name"].(string), tag).Err()
	if err != nil {
		return err
	}

	return nil
}

func (c *Redis) Get(key string) (map[string]string, error) {
	m, err := c.redis.HGetAll(key).Result()
	if err == redis.Nil {
		log.Printf("Article does not exist")
	} else if err != nil {
		log.Printf("Some Error Occured, %s", key)
		return m, err
	}

	return m, nil
}

func (c *Redis) Scan(prefix string) ([]string, error) {
	var cursor uint64
	var keys []string

	// Scan keys
	for {
		var err error
		var t []string
		t, cursor, err = c.redis.Scan(cursor, prefix+"*", 1000).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, t...)
		if cursor == 0 {
			break
		}
	}
	if len(keys) == 0 {
		return []string{"Empty"}, nil
	}
	return keys, nil
}

func (c *Redis) Update(key string, field string, value interface{}) error {
	_, err := c.redis.HSet(key, field, value).Result()
	if err != nil {
		return err
	}
	return nil
}

func (c *Redis) Delete(key string) error {
	c.redis.Del(key)
	return nil
}
