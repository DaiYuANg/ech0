package broker

type WebhookSinkConfig struct {
	Name         string                    `json:"name"          koanf:"name"          mapstructure:"name"          toml:"name"`
	Tenant       string                    `json:"tenant"        koanf:"tenant"        mapstructure:"tenant"        toml:"tenant"`
	Namespace    string                    `json:"namespace"     koanf:"namespace"     mapstructure:"namespace"     toml:"namespace"`
	Principal    string                    `json:"principal"     koanf:"principal"     mapstructure:"principal"     toml:"principal"`
	Topic        string                    `json:"topic"         koanf:"topic"         mapstructure:"topic"         toml:"topic"`
	Partition    uint32                    `json:"partition"     koanf:"partition"     mapstructure:"partition"     toml:"partition"`
	Consumer     string                    `json:"consumer"      koanf:"consumer"      mapstructure:"consumer"      toml:"consumer"`
	URL          string                    `json:"url"           koanf:"url"           mapstructure:"url"           toml:"url"`
	Method       string                    `json:"method"        koanf:"method"        mapstructure:"method"        toml:"method"`
	IntervalSecs uint64                    `json:"interval_secs" koanf:"interval_secs" mapstructure:"interval_secs" toml:"interval_secs"`
	MaxRecords   int                       `json:"max_records"   koanf:"max_records"   mapstructure:"max_records"   toml:"max_records"`
	TimeoutMS    uint64                    `json:"timeout_ms"    koanf:"timeout_ms"    mapstructure:"timeout_ms"    toml:"timeout_ms"`
	Headers      []WebhookSinkHeaderConfig `json:"headers"       koanf:"headers"       mapstructure:"headers"       toml:"headers"`
}

type WebhookSinkHeaderConfig struct {
	Key   string `json:"key"   koanf:"key"   mapstructure:"key"   toml:"key"`
	Value string `json:"value" koanf:"value" mapstructure:"value" toml:"value"`
}
