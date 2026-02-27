package drivers

type Remote struct {
	Type              string
	ID                string
	URL               string
	Tag               string
	User              string
	Password          string
	Path              string
	ResolvedLocalPath string
}

type PushRequest struct {
	Repository   string
	Reference    string
	User         string
	Password     string
	Payload      []byte
	PayloadMedia string
	ArtifactType string
}
