package cache_buntdb

import (
	"github.com/chefsgo/chef"
)

func Driver(ss ...string) chef.CacheDriver {
	store := ""
	if len(ss) > 0 {
		store = ss[0]
	}
	return &buntdbCacheDriver{store}
}

func init() {
	chef.Register("buntdb", Driver("store/cache.db"))
}
