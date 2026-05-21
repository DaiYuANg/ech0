package broker

type DatabaseOutboxConfig struct {
	Name             string                    `json:"name"               koanf:"name"               mapstructure:"name"               toml:"name"`
	Tenant           string                    `json:"tenant"             koanf:"tenant"             mapstructure:"tenant"             toml:"tenant"`
	Namespace        string                    `json:"namespace"          koanf:"namespace"          mapstructure:"namespace"          toml:"namespace"`
	Principal        string                    `json:"principal"          koanf:"principal"          mapstructure:"principal"          toml:"principal"`
	Topic            string                    `json:"topic"              koanf:"topic"              mapstructure:"topic"              toml:"topic"`
	DriverName       string                    `json:"driver_name"        koanf:"driver_name"        mapstructure:"driver_name"        toml:"driver_name"`
	DSN              string                    `json:"dsn"                koanf:"dsn"                mapstructure:"dsn"                toml:"dsn"`
	Query            string                    `json:"query"              koanf:"query"              mapstructure:"query"              toml:"query"`
	MarkDeliveredSQL string                    `json:"mark_delivered_sql" koanf:"mark_delivered_sql" mapstructure:"mark_delivered_sql" toml:"mark_delivered_sql"`
	IntervalSecs     uint64                    `json:"interval_secs"      koanf:"interval_secs"      mapstructure:"interval_secs"      toml:"interval_secs"`
	MaxRecords       int                       `json:"max_records"        koanf:"max_records"        mapstructure:"max_records"        toml:"max_records"`
	Headers          []WebhookSinkHeaderConfig `json:"headers"            koanf:"headers"            mapstructure:"headers"            toml:"headers"`
}
