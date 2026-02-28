package app

type Settings struct {
	TargetDir string    `yaml:"targetDir,omitempty"`
	Packages  []Package `yaml:"packages"`
}

type Package struct {
	Type      string `yaml:"type"`
	Id        string `yaml:"id"`
	URL       string `yaml:"url"`
	Tag       string `yaml:"tag,omitempty"`
	User      string `yaml:"user,omitempty"`
	Password  string `yaml:"password,omitempty"`
	Path      string `yaml:"path,omitempty"`
	LocalPath string `yaml:"localPath,omitempty"`

	ResolvedLocalPath string `yaml:"-"`
}

func (s *Settings) HasPackage(id string) bool {
	for _, r := range s.Packages {
		if r.Id == id {
			return true
		}
	}
	return false
}
