package user

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type userEvent struct {
	Type   string `json:"type"`
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

// Projector turns user.events into profile rows.
type Projector struct {
	store *Store
	log   *slog.Logger
}

func NewProjector(store *Store, log *slog.Logger) *Projector {
	return &Projector{store: store, log: log}
}

func (p *Projector) Handle(ctx context.Context, recs []*kgo.Record) error {
	for _, r := range recs {
		var ev userEvent
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			p.log.Error("skipping bad user event", "partition", r.Partition, "offset", r.Offset, "error", err)
			continue
		}
		if ev.Type != "user.created" {
			continue
		}
		if _, err := uuid.Parse(ev.UserID); err != nil || ev.Email == "" {
			p.log.Error("skipping invalid user.created event", "partition", r.Partition, "offset", r.Offset)
			continue
		}
		if err := p.store.CreateFromEvent(ctx, ev.UserID, ev.Email); err != nil {
			return err // transient: the whole batch is retried
		}
	}
	return nil
}
