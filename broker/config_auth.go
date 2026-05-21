package broker

import "strings"

func normalizeAuthConfig(cfg *AuthConfig) {
	enabledByToken := false
	for index := range cfg.StaticTokens {
		token := &cfg.StaticTokens[index]
		token.Token = strings.TrimSpace(token.Token)
		token.Principal = strings.TrimSpace(token.Principal)
		token.Tenant = strings.TrimSpace(token.Tenant)
		token.Namespace = strings.TrimSpace(token.Namespace)
		token.ClientID = strings.TrimSpace(token.ClientID)
		token.Instance = strings.TrimSpace(token.Instance)
		if token.Token != "" {
			enabledByToken = true
		}
	}
	if enabledByToken {
		cfg.Enabled = true
		cfg.AllowAnonymous = false
	}
	if !cfg.Enabled {
		cfg.AllowAnonymous = true
	}
}
