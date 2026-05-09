package broker

import "context"

type Builder struct {
	cfg Config
}

func NewBuilder() *Builder {
	return &Builder{cfg: DefaultConfig()}
}

func BuilderFromConfig(cfg Config) *Builder {
	normalizeConfig(&cfg)
	return &Builder{cfg: cfg}
}

func BuilderFromEnv() (*Builder, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	return BuilderFromConfig(cfg), nil
}

func (b *Builder) WithConfig(update func(*Config)) *Builder {
	if update != nil {
		update(&b.cfg)
		normalizeConfig(&b.cfg)
	}
	return b
}

func (b *Builder) DataDir(value string) *Builder {
	b.cfg.Broker.DataDir = value
	return b
}

func (b *Builder) BrokerBindAddr(value string) *Builder {
	b.cfg.Broker.BindAddr = value
	return b
}

func (b *Builder) AdminEnabled(enabled bool) *Builder {
	b.cfg.Admin.Enabled = enabled
	return b
}

func (b *Builder) AdminBindAddr(value string) *Builder {
	b.cfg.Admin.BindAddr = value
	return b
}

func (b *Builder) Config() Config {
	return b.cfg
}

func (b *Builder) New(opts ...Option) (*Broker, error) {
	return New(b.cfg, opts...)
}

func (b *Builder) Run(ctx context.Context) error {
	return RunWithConfig(ctx, b.cfg)
}
