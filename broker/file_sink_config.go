package broker

type FileSinkConfig struct {
	Name         string `json:"name"          koanf:"name"          mapstructure:"name"          toml:"name"`
	Tenant       string `json:"tenant"        koanf:"tenant"        mapstructure:"tenant"        toml:"tenant"`
	Namespace    string `json:"namespace"     koanf:"namespace"     mapstructure:"namespace"     toml:"namespace"`
	Principal    string `json:"principal"     koanf:"principal"     mapstructure:"principal"     toml:"principal"`
	Topic        string `json:"topic"         koanf:"topic"         mapstructure:"topic"         toml:"topic"`
	Partition    uint32 `json:"partition"     koanf:"partition"     mapstructure:"partition"     toml:"partition"`
	Consumer     string `json:"consumer"      koanf:"consumer"      mapstructure:"consumer"      toml:"consumer"`
	Path         string `json:"path"          koanf:"path"          mapstructure:"path"          toml:"path"`
	IntervalSecs uint64 `json:"interval_secs" koanf:"interval_secs" mapstructure:"interval_secs" toml:"interval_secs"`
	MaxRecords   int    `json:"max_records"   koanf:"max_records"   mapstructure:"max_records"   toml:"max_records"`
}
