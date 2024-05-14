package model

type AdvertEvent struct {
	Id      int64  `json:"id"`
	Type    string `json:"type"`
	Version int16  `json:"version"`
}
