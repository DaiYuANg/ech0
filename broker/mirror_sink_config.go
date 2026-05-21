package broker

type MirrorSinkConfig struct {
	Name            string                    `json:"name"             koanf:"name"             mapstructure:"name"             toml:"name"`
	Tenant          string                    `json:"tenant"           koanf:"tenant"           mapstructure:"tenant"           toml:"tenant"`
	Namespace       string                    `json:"namespace"        koanf:"namespace"        mapstructure:"namespace"        toml:"namespace"`
	Principal       string                    `json:"principal"        koanf:"principal"        mapstructure:"principal"        toml:"principal"`
	Topic           string                    `json:"topic"            koanf:"topic"            mapstructure:"topic"            toml:"topic"`
	TargetTopic     string                    `json:"target_topic"     koanf:"target_topic"     mapstructure:"target_topic"     toml:"target_topic"`
	Partition       uint32                    `json:"partition"        koanf:"partition"        mapstructure:"partition"        toml:"partition"`
	Consumer        string                    `json:"consumer"         koanf:"consumer"         mapstructure:"consumer"         toml:"consumer"`
	TargetAdminURL  string                    `json:"target_admin_url" koanf:"target_admin_url" mapstructure:"target_admin_url" toml:"target_admin_url"`
	TargetTenant    string                    `json:"target_tenant"    koanf:"target_tenant"    mapstructure:"target_tenant"    toml:"target_tenant"`
	TargetNamespace string                    `json:"target_namespace" koanf:"target_namespace" mapstructure:"target_namespace" toml:"target_namespace"`
	TargetPrincipal string                    `json:"target_principal" koanf:"target_principal" mapstructure:"target_principal" toml:"target_principal"`
	AuthToken       string                    `json:"auth_token"       koanf:"auth_token"       mapstructure:"auth_token"       toml:"auth_token"`
	IntervalSecs    uint64                    `json:"interval_secs"    koanf:"interval_secs"    mapstructure:"interval_secs"    toml:"interval_secs"`
	MaxRecords      int                       `json:"max_records"      koanf:"max_records"      mapstructure:"max_records"      toml:"max_records"`
	TimeoutMS       uint64                    `json:"timeout_ms"       koanf:"timeout_ms"       mapstructure:"timeout_ms"       toml:"timeout_ms"`
	Headers         []WebhookSinkHeaderConfig `json:"headers"          koanf:"headers"          mapstructure:"headers"          toml:"headers"`
}
