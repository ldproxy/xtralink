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
	// ManifestPath is where the content of the local mirror is recorded
	// after each pull and push (s. Manifest). With it, SyncBack carries only
	// the changes made to the mirror; without it, SyncBack mirrors the whole
	// local directory, deleting whatever else the remote holds.
	ManifestPath string
}

type PushRequest struct {
	Source    Remote
	Target    Remote
	TargetTag string
}
