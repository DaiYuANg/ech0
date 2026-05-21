package broker

type S3SinkConfig struct {
	Name            string                    `json:"name"              koanf:"name"              mapstructure:"name"              toml:"name"`
	Tenant          string                    `json:"tenant"            koanf:"tenant"            mapstructure:"tenant"            toml:"tenant"`
	Namespace       string                    `json:"namespace"         koanf:"namespace"         mapstructure:"namespace"         toml:"namespace"`
	Principal       string                    `json:"principal"         koanf:"principal"         mapstructure:"principal"         toml:"principal"`
	Topic           string                    `json:"topic"             koanf:"topic"             mapstructure:"topic"             toml:"topic"`
	Partition       uint32                    `json:"partition"         koanf:"partition"         mapstructure:"partition"         toml:"partition"`
	Consumer        string                    `json:"consumer"          koanf:"consumer"          mapstructure:"consumer"          toml:"consumer"`
	EndpointURL     string                    `json:"endpoint_url"      koanf:"endpoint_url"      mapstructure:"endpoint_url"      toml:"endpoint_url"`
	Bucket          string                    `json:"bucket"            koanf:"bucket"            mapstructure:"bucket"            toml:"bucket"`
	Prefix          string                    `json:"prefix"            koanf:"prefix"            mapstructure:"prefix"            toml:"prefix"`
	Region          string                    `json:"region"            koanf:"region"            mapstructure:"region"            toml:"region"`
	AccessKeyID     string                    `json:"access_key_id"     koanf:"access_key_id"     mapstructure:"access_key_id"     toml:"access_key_id"`
	SecretAccessKey string                    `json:"secret_access_key" koanf:"secret_access_key" mapstructure:"secret_access_key" toml:"secret_access_key"`
	SessionToken    string                    `json:"session_token"     koanf:"session_token"     mapstructure:"session_token"     toml:"session_token"`
	IntervalSecs    uint64                    `json:"interval_secs"     koanf:"interval_secs"     mapstructure:"interval_secs"     toml:"interval_secs"`
	MaxRecords      int                       `json:"max_records"       koanf:"max_records"       mapstructure:"max_records"       toml:"max_records"`
	TimeoutMS       uint64                    `json:"timeout_ms"        koanf:"timeout_ms"        mapstructure:"timeout_ms"        toml:"timeout_ms"`
	Headers         []WebhookSinkHeaderConfig `json:"headers"           koanf:"headers"           mapstructure:"headers"           toml:"headers"`
}
