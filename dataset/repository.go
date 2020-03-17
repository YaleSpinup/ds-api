package dataset

// Repository is information about a data repository
type Repository struct {
	Name string `json:"name"`
	Tags []*Tag `json:"tags"`
}

// Tag is the structure of a data repository tag
type Tag struct {
	Key   *string `json:"key"`
	Value *string `json:"value"`
}
