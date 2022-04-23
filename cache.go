package cache_buntdb

import (
	"errors"
	"os"
	"path"
	"sync"
	"time"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/cache"
	"github.com/chefsgo/codec"
	"github.com/tidwall/buntdb"
)

var (
	errInvalidCacheConnection = errors.New("Invalid cache connection.")
)

type (
	buntdbDriver struct {
		store string
	}
	buntdbConnect struct {
		mutex sync.RWMutex

		name    string
		config  cache.Config
		setting buntdbSetting

		db *buntdb.DB
	}
	buntdbSetting struct {
		Store string
	}
	buntdbValue struct {
		Value Any `json:"value"`
	}
)

//连接
func (driver *buntdbDriver) Connect(name string, config cache.Config) (cache.Connect, error) {
	//获取配置信息
	setting := buntdbSetting{
		Store: driver.store,
	}

	//创建目录，如果不存在
	dir := path.Dir(setting.Store)
	_, e := os.Stat(dir)
	if e != nil {
		os.MkdirAll(dir, 0700)
	}

	if vv, ok := config.Setting["file"].(string); ok && vv != "" {
		setting.Store = vv
	}
	if vv, ok := config.Setting["store"].(string); ok && vv != "" {
		setting.Store = vv
	}

	return &buntdbConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *buntdbConnect) Open() error {
	if connect.setting.Store == "" {
		return errors.New("无效缓存存储")
	}
	db, err := buntdb.Open(connect.setting.Store)
	if err != nil {
		return err
	}
	connect.db = db
	return nil
}

//关闭连接
func (connect *buntdbConnect) Close() error {
	if connect.db != nil {
		if err := connect.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

//查询缓存，
func (connect *buntdbConnect) Read(key string) (Any, error) {
	if connect.db == nil {
		return nil, errInvalidCacheConnection
	}

	realKey := connect.config.Prefix + key
	realVal := ""

	err := connect.db.View(func(tx *buntdb.Tx) error {
		vvv, err := tx.Get(realKey)
		if err != nil {
			return err
		}
		realVal = vvv
		return nil
	})
	if err == buntdb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	mcv := buntdbValue{}

	err = codec.UnmarshalJSON([]byte(realVal), &mcv)
	if err != nil {
		return nil, nil
	}

	return mcv.Value, nil
}

//更新缓存
func (connect *buntdbConnect) Write(key string, val Any, expiry time.Duration) error {
	if connect.db == nil {
		return errInvalidCacheConnection
	}

	value := buntdbValue{val}

	bytes, err := codec.MarshalJSON(value)
	if err != nil {
		return err
	}

	realVal := string(bytes)

	return connect.db.Update(func(tx *buntdb.Tx) error {
		opts := &buntdb.SetOptions{Expires: false}
		if expiry > 0 {
			opts.Expires = true
			opts.TTL = expiry
		}
		_, _, err := tx.Set(key, realVal, opts)
		return err
	})
}

//查询缓存，
func (connect *buntdbConnect) Exists(key string) (bool, error) {
	if connect.db == nil {
		return false, errInvalidCacheConnection
	}

	err := connect.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		return err
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return true, nil
		}
	}
	return false, nil
}

//删除缓存
func (connect *buntdbConnect) Delete(key string) error {
	if connect.db == nil {
		return errInvalidCacheConnection
	}

	return connect.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
}

func (connect *buntdbConnect) Serial(key string, start, step int64) (int64, error) {
	//加并发锁，忘记之前为什么加了，应该是有问题加了才正常的
	// connect.mutex.Lock()
	// defer connect.mutex.Unlock()

	value := start

	if val, err := connect.Read(key); err == nil {
		if vv, ok := val.(float64); ok {
			value = int64(vv)
		} else if vv, ok := val.(int64); ok {
			value = vv
		}
	}

	//加数字
	value += step

	//写入值，这个应该不过期
	err := connect.Write(key, value, 0)
	if err != nil {
		return int64(0), err
	}

	return value, nil
}

func (connect *buntdbConnect) Clear(prefix string) error {
	if connect.db == nil {
		return errors.New("连接失败")
	}

	keys, err := connect.Keys(prefix)
	if err != nil {
		return err
	}

	return connect.db.Update(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			_, err := tx.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
func (connect *buntdbConnect) Keys(prefix string) ([]string, error) {
	if connect.db == nil {
		return nil, errors.New("连接失败")
	}

	keys := []string{}
	err := connect.db.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys(prefix+"*", func(k, v string) bool {
			keys = append(keys, k)
			return true
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}
