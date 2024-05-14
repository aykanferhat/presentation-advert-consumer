package model_repository

type Advert struct {
	Id          int64          `json:"id"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Version     int16          `json:"version"`
	Category    AdvertCategory `json:"category"`
}

type AdvertCategory struct {
	Id      int64  `json:"id"`
	Name    string `json:"name"`
	Version int16  `json:"version"`
}
