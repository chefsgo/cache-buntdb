package cache_buntdb

import (
	"github.com/chefsgo/cache"
)

func Driver(ss ...string) cache.Driver {
	store := ""
	if len(ss) > 0 {
		store = ss[0]
	}
	return &buntdbDriver{store}
}

func init() {
	cache.Register("buntdb", Driver("store/cache.db"))
}
