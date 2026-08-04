//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	amqpv1 "github.com/KFN002/perfect-go-service/internal/controller/amqp/v1"
	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/repo/auditstore"
	"github.com/KFN002/perfect-go-service/internal/repo/cache"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/bulkhead"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
)

// TestAuditFullPipeline is the "full audit test": real PG18 + Redis 8 +
// RabbitMQ 4, the production ingest pipeline end to end.
//
// It asserts, in order:
//  1. every published event lands durably in the partitioned store,
//  2. duplicates (at-least-once redelivery) collapse to one row,
//  3. poison messages route to the DLQ without wedging the pipeline,
//  4. the store is truly append-only (UPDATE/DELETE rejected by trigger),
//  5. keyset pagination walks the full set without gaps or overlaps,
//  6. stats aggregate correctly.
func TestAuditFullPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool := startPostgres(t, ctx, "audit")
	migrateAudit(t, pool)
	rds := startRedis(t, ctx)
	mq := startRabbit(t, ctx)

	if err := mq.DeclareFlow(amqpv1.AuditFlow); err != nil {
		t.Fatalf("declare flow: %v", err)
	}
	pub, err := mq.NewPublisher()
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer pub.Close()

	log := zap.NewNop()
	store := auditstore.New(pool)
	ingestor := audit.NewIngestor(audit.IngestConfig{
		BatchMaxSize: 50,
		BatchMaxWait: 50 * time.Millisecond,
	}, store, cache.NewDeduper(rds, log), log)
	defer ingestor.Close()

	// Run the production consumer.
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = amqpv1.RunAuditConsumer(consumerCtx, mq, pub, ingestor,
			bulkhead.New("test-ingest", 32), 128)
	})

	// ---- 1+2: publish 200 unique events, each twice (duplicate delivery) ---
	const unique = 200
	ids := make([]uuid.UUID, unique)
	for i := range unique {
		msg := messages.NewAudit(entity.AuditTaskDone, "integration-test",
			"task", uuid.NewString(), "", map[string]any{"n": i})
		ids[i] = msg.ID
		body, _ := jsonx.Marshal(msg)
		for rep := range 2 {
			if err := pub.Publish(ctx, amqpv1.AuditFlow.Exchange, amqpv1.AuditFlow.RoutingKey,
				rabbitmq.Message{Body: body, EventID: msg.ID.String()}); err != nil {
				t.Fatalf("publish %d/%d: %v", i, rep, err)
			}
		}
	}

	// ---- 3: poison message ------------------------------------------------
	if err := pub.Publish(ctx, amqpv1.AuditFlow.Exchange, amqpv1.AuditFlow.RoutingKey,
		rabbitmq.Message{Body: []byte("this is not json{{{")}); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	// Wait until all unique rows are visible.
	deadline := time.Now().Add(60 * time.Second)
	var total int64
	for time.Now().Before(deadline) {
		stats, err := store.Stats(ctx)
		if err == nil {
			total = stats.Total
			if total >= unique {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if total != unique {
		t.Fatalf("stored rows = %d, want exactly %d (dedup must collapse duplicates)", total, unique)
	}

	// ---- 3 assert: poison ended in the DLQ --------------------------------
	poisonDeadline := time.Now().Add(30 * time.Second)
	var dlqLen int
	for time.Now().Before(poisonDeadline) {
		items, err := mq.InspectDLQ(amqpv1.AuditFlow, 10)
		if err == nil {
			dlqLen = len(items)
			if dlqLen > 0 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if dlqLen != 1 {
		t.Errorf("DLQ has %d messages, want 1 (the poison)", dlqLen)
	}

	// ---- 4: immutability guard --------------------------------------------
	if _, err := pool.Exec(ctx, "UPDATE audit_events SET actor = 'tamper'"); err == nil {
		t.Fatal("UPDATE on audit_events succeeded — append-only guard broken")
	}
	if _, err := pool.Exec(ctx, "DELETE FROM audit_events"); err == nil {
		t.Fatal("DELETE on audit_events succeeded — append-only guard broken")
	}

	// ---- 5: keyset pagination walks everything exactly once ---------------
	seen := make(map[uuid.UUID]bool)
	filter := entity.AuditFilter{Limit: 37}
	for {
		page, err := store.Query(ctx, filter)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, ev := range page {
			if seen[ev.ID] {
				t.Fatalf("event %s returned twice — keyset overlap", ev.ID)
			}
			seen[ev.ID] = true
		}
		last := page[len(page)-1]
		filter.CursorTime = last.OccurredAt
		filter.CursorID = last.ID
	}
	if len(seen) != unique {
		t.Errorf("pagination visited %d events, want %d", len(seen), unique)
	}

	// ---- 6: stats -----------------------------------------------------------
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ByType[string(entity.AuditTaskDone)] != unique {
		t.Errorf("by_type[task.done] = %d, want %d", stats.ByType[string(entity.AuditTaskDone)], unique)
	}

	stopConsumer()
	wg.Wait()
}

// TestAuditPartitions verifies the maintainer pre-creates daily partitions.
func TestAuditPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := startPostgres(t, ctx, "audit")
	migrateAudit(t, pool)
	store := auditstore.New(pool)

	if err := store.EnsurePartitions(ctx, 7); err != nil {
		t.Fatalf("ensure partitions: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'audit_events_2%'`).Scan(&n); err != nil {
		t.Fatalf("count partitions: %v", err)
	}
	// Migration bootstraps 2 (today+tomorrow); maintainer extends to 8 total.
	if n < 8 {
		t.Errorf("partitions = %d, want ≥ 8", n)
	}

	// Insert lands in the right partition and is queryable.
	ev := entity.AuditEvent{
		ID: uuid.New(), OccurredAt: time.Now().UTC(),
		Type: entity.AuditAPIAccess, Service: "test",
	}
	if _, err := store.InsertBatch(ctx, []entity.AuditEvent{ev}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := store.Query(ctx, entity.AuditFilter{Limit: 10})
	if err != nil || len(got) != 1 {
		t.Fatalf("query after insert: %v (%d rows)", err, len(got))
	}
}

// TestAuditDedupSurvivesRedisLoss proves PG ON CONFLICT is the dedup backstop
// when Redis knows nothing (cold cache, flushed keys).
func TestAuditDedupSurvivesRedisLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := startPostgres(t, ctx, "audit")
	migrateAudit(t, pool)
	store := auditstore.New(pool)

	ev := entity.AuditEvent{
		ID: uuid.New(), OccurredAt: time.Now().UTC().Truncate(time.Microsecond),
		Type: entity.AuditTaskDone, Service: "test",
	}
	first, err := store.InsertBatch(ctx, []entity.AuditEvent{ev})
	if err != nil || first != 1 {
		t.Fatalf("first insert: n=%d err=%v", first, err)
	}
	second, err := store.InsertBatch(ctx, []entity.AuditEvent{ev})
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if second != 0 {
		t.Errorf("duplicate insert wrote %d rows, want 0", second)
	}
	_ = constants.AuditDedupTTL // documents the Redis fast path this test bypasses
}
