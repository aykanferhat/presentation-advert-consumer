package model_cache

import "time"

type Category struct {
	Id               int64     `json:"id"`
	Name             string    `json:"name"`
	Version          int16     `json:"version"`
	CacheUpdatedDate time.Time `json:"cacheUpdatedDate"`
}
