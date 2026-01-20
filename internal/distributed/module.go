package distributed

import (
  "log/slog"

  "github.com/hashicorp/go-uuid"
  "go.uber.org/fx"
)

var Module = fx.Module("distributed", fx.Provide(
  func(logger *slog.Logger) *Store {
    store := New(true, logger)
    store.RaftBind = "localhost:12000"
    return store
  },
),
  fx.Invoke(open),
)

func open(s *Store) error {
  generateUUID, err := uuid.GenerateUUID()
  if err != nil {
    return err
  }
  return s.Open(true, generateUUID)
}
