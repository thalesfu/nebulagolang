package nebulagolang

type TagEntity interface {
	VertexEntity
	GetTagName() string
	New() TagEntity
}

type MultiTagEntity interface {
	VertexEntity
	GetTags() []TagEntity
}
