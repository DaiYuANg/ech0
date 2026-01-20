package config

type Config struct {
	Metrics Metrics `koanf:"metrics"`
	Logger  Logger  `koanf:"logger"`
}

type Metrics struct {
	Prefix string `koanf:"prefix"`
}
