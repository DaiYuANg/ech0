package config

type Console struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

type File struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Path     string `json:"path" yaml:"path"`
	MaxSize  int    `json:"max_size" yaml:"max_size"`
	MaxAge   int    `json:"max_age" yaml:"max_age"`
	MaxFiles int    `json:"max_files" yaml:"max_files"`
}

type Logger struct {
	Level   string  `json:"level" yaml:"level"`
	Console Console `json:"console" yaml:"console"`
	File    File    `json:"file" yaml:"file"`
}
