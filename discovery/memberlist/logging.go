package memberlist

import (
	"log/slog"
	"strings"
)

type slogWriter struct {
	logger *slog.Logger
}

func (w slogWriter) Write(p []byte) (int, error) {
	if w.logger == nil {
		return len(p), nil
	}
	message := strings.TrimSpace(string(p))
	if message != "" {
		w.logger.Debug("memberlist", "message", message)
	}
	return len(p), nil
}
