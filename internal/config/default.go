package config

func defaultConfig() Config {
	return Config{
		Logger: Logger{
			Level: "debug",
			Console: Console{
				Enabled: true,
			},
			File: File{Enabled: false},
		},
		Metrics: Metrics{Prefix: "/premetheus"},
	}
}
