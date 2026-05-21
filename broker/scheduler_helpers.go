package broker

import "log/slog"

func logMoved(logger *slog.Logger, message string, moved int) {
	if moved > 0 && logger != nil {
		logger.Info(message, "moved", moved)
	}
}
