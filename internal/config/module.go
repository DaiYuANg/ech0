package config

import (
	"strings"

	"github.com/DaiYuANg/ech0/constant"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
	"go.uber.org/fx"
)

var Module = fx.Module("config", fx.Provide(
	newKoanf,
	loadConfig,
))

type LoadResult struct {
	fx.Out
	Config  *Config
	Metrics *Metrics
	Logger  *Logger
}

func newKoanf() *koanf.Koanf {
	return koanf.New(".")
}

func loadConfig(k *koanf.Koanf) (LoadResult, error) {
	def := defaultConfig()

	// 加载默认配置
	if err := k.Load(structs.Provider(def, "koanf"), nil); err != nil {
		return LoadResult{}, err
	}

	// 使用 lo.Ternary 优化字符串映射函数
	mapEnvKey := func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, constant.EnvPrefix)), "_", ".")
	}
	if err := k.Load(env.Provider(constant.EnvPrefix, ".", mapEnvKey), nil); err != nil {
		return LoadResult{}, err
	}

	if err := k.Unmarshal("", &def); err != nil {
		return LoadResult{}, err
	}

	return LoadResult{
		Config:  &def,
		Logger:  &def.Logger,
		Metrics: &def.Metrics,
	}, nil
}
