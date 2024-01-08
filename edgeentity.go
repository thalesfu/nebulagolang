package nebulagolang

type EdgeEntity interface {
	GetEdgeName() string
	StartVID() string
	EndVID() string
	EID() string
	SetStartVID(vid string)
	SetEndVID(vid string)
}
