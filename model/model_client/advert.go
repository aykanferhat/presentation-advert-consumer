package model_client

type AdvertResponse struct {
	Id          int64  `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	CategoryId  int64  `json:"categoryId"`
	Version     int16  `json:"version"`
}
