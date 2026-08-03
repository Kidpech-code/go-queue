package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// เทสที่เกี่ยวกับเวลาใช้ testing/synctest (Go 1.25+): นาฬิกาปลอมทั้ง bubble
// → เร็ว, deterministic, ไม่ต้องฉีด Clock interface เข้าไปในโค้ดโปรดักชัน

// ── ตรรกะการเรียงลำดับ ──────────────────────────────────────────────

func TestPriorityThenFIFO(t *testing.T) {
	q := NewMemQueue(100, 3, time.Minute)
	in := []struct {
		id string
		p  int
	}{{"lo1", 0}, {"hi1", 9}, {"mid1", 5}, {"lo2", 0}, {"hi2", 9}}
	for _, s := range in {
		if err := q.Enqueue(&Job{ID: s.id, Priority: s.p}); err != nil {
			t.Fatalf("Enqueue(%s) = %v", s.id, err)
		}
	}

	var got []string
	for range in {
		j, err := q.Dequeue(t.Context())
		if err != nil {
			t.Fatalf("Dequeue = %v", err)
		}
		got = append(got, j.ID)
	}

	// priority สูงมาก่อน; priority เท่ากันต้องเรียงตามลำดับที่เข้ามา (heap ต้องมี tie-break)
	want := "[hi1 hi2 mid1 lo1 lo2]"
	if fmt.Sprint(got) != want {
		t.Errorf("ลำดับ = %v, ต้องการ %s", got, want)
	}
}

// ── ตรรกะเรื่องเวลา ────────────────────────────────────────────────

func TestDelayedJobNotServedEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(100, 3, time.Minute)
		start := time.Now()
		q.Enqueue(&Job{ID: "later", RunAt: start.Add(30 * time.Second)})

		if s := q.Stats(); s.Ready != 0 || s.Delayed != 1 {
			t.Fatalf("ก่อนถึงเวลาต้องอยู่ delayed: %+v", s)
		}

		j, err := q.Dequeue(t.Context()) // ต้องบล็อกจนนาฬิกาปลอมเดินถึง RunAt
		if err != nil || j.ID != "later" {
			t.Fatalf("Dequeue = %v, %v", j, err)
		}
		if waited := time.Since(start); waited != 30*time.Second {
			t.Errorf("รอ %v, ต้องการ 30s พอดี", waited)
		}
	})
}

func TestLeaseExpiryRedelivers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const vt = 30 * time.Second
		q := NewMemQueue(100, 5, vt)
		q.Enqueue(&Job{ID: "j7"})

		// worker A รับงานไปแล้ว "ตาย" — ไม่ Ack ไม่ Nack
		a, err := q.Dequeue(t.Context())
		if err != nil || a.Attempt != 1 {
			t.Fatalf("ครั้งแรก = %+v, %v (Attempt ต้อง = 1)", a, err)
		}
		if s := q.Stats(); s.Inflight != 1 {
			t.Fatalf("ต้องอยู่ inflight: %+v", s)
		}

		// worker B ต้องได้งานเดิมกลับมาหลัง lease หมด — นี่คือ at-least-once
		start := time.Now()
		b, err := q.Dequeue(t.Context())
		if err != nil || b.ID != "j7" || b.Attempt != 2 {
			t.Fatalf("ส่งซ้ำ = %+v, %v (ต้องเป็น j7 attempt=2)", b, err)
		}
		if waited := time.Since(start); waited != vt {
			t.Errorf("ส่งซ้ำหลัง %v, ต้องการ %v", waited, vt)
		}

		if err := q.Ack("j7"); err != nil {
			t.Errorf("Ack = %v", err)
		}
		// worker A ที่ฟื้นขึ้นมาแล้ว Ack ต้องรู้ว่างานหลุดมือไปแล้ว
		if err := q.Ack("j7"); !errors.Is(err, ErrNotInflight) {
			t.Errorf("Ack ซ้ำ = %v, ต้องการ ErrNotInflight", err)
		}
	})
}

// ── ตรรกะความล้มเหลว ───────────────────────────────────────────────

func TestRetryUntilDLQ(t *testing.T) {
	const maxAttempt = 3
	q := NewMemQueue(100, maxAttempt, time.Minute)
	q.Enqueue(&Job{ID: "poison"})

	for i := 1; i <= maxAttempt; i++ {
		j, err := q.Dequeue(t.Context())
		if err != nil {
			t.Fatalf("รอบ %d: Dequeue = %v", i, err)
		}
		if j.Attempt != i {
			t.Fatalf("รอบ %d: Attempt = %d", i, j.Attempt)
		}
		if err := q.Nack(j.ID, 0, errors.New("เทส")); err != nil {
			t.Fatalf("รอบ %d: Nack = %v", i, err)
		}
	}

	s := q.Stats()
	if s.Dead != 1 || s.Ready != 0 || s.Delayed != 0 || s.Inflight != 0 {
		t.Fatalf("ต้องอยู่ DLQ อย่างเดียว: %+v", s)
	}
	if got := q.Dead()[0].Attempt; got != maxAttempt {
		t.Errorf("Attempt ที่ DLQ = %d, ต้องการ %d", got, maxAttempt)
	}
}

// DLQ ที่วินิจฉัยไม่ได้ = ไร้ประโยชน์ — §6.8 บอกให้ดู last_error ตัวแรก
func TestDLQCarriesFailureReason(t *testing.T) {
	q := NewMemQueue(10, 2, time.Minute)
	q.Enqueue(&Job{ID: "p"})

	j, _ := q.Dequeue(t.Context())
	q.Nack(j.ID, 0, errors.New("ครั้งแรก: connection refused"))
	j, _ = q.Dequeue(t.Context())
	q.Nack(j.ID, 0, errors.New("ครั้งสอง: no such host"))

	dead := q.Dead()
	if len(dead) != 1 {
		t.Fatalf("DLQ = %d ตัว", len(dead))
	}
	if dead[0].LastErr != "ครั้งสอง: no such host" {
		t.Errorf("LastErr = %q, ต้องเป็นสาเหตุครั้งล่าสุด", dead[0].LastErr)
	}
}

// DLQ ต้องมีขอบ ไม่งั้นรั่วจน OOM เมื่อไม่มีใครระบาย
func TestDLQIsBounded(t *testing.T) {
	const cap = 3
	q := NewMemQueue(cap, 1, time.Minute) // maxAttempt=1 → nack ครั้งเดียวลง DLQ
	for i := range cap + 2 {
		id := fmt.Sprintf("p%d", i)
		if err := q.Enqueue(&Job{ID: id}); err != nil {
			t.Fatalf("Enqueue(%s) = %v", id, err)
		}
		j, err := q.Dequeue(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		q.Nack(j.ID, 0, errors.New("boom"))
	}

	s := q.Stats()
	if s.Dead != cap { // Fatalf ไม่ใช่ Errorf — บรรทัดล่างอ่าน Dead()[0] ซึ่ง panic ถ้า DLQ ว่าง
		t.Fatalf("DLQ = %d, ต้องหยุดที่ capacity = %d", s.Dead, cap)
	}
	if s.DeadDropped != 2 {
		t.Errorf("DeadDropped = %d, ต้องการ 2", s.DeadDropped)
	}
	// เก็บ N ตัวแรกไว้วินิจฉัย — ตัวหลังมักสาเหตุเดียวกัน
	if got := q.Dead()[0].ID; got != "p0" {
		t.Errorf("ตัวแรกใน DLQ = %s, ต้องเป็น p0 (ตัวเก่าสุด)", got)
	}
}

func TestBackoffBounds(t *testing.T) {
	// full jitter: ผลลัพธ์ต้องอยู่ใน [0, min(base<<attempt, ceiling)] เสมอ
	for _, attempt := range []int{1, 2, 5, 10, 50, 1000} {
		ceiling := min(100*time.Millisecond<<min(attempt, 20), 30*time.Second)
		for range 200 {
			d := Backoff(attempt)
			if d < 0 || d > ceiling {
				t.Fatalf("Backoff(%d) = %v, ต้องอยู่ใน [0, %v]", attempt, d, ceiling)
			}
		}
	}
}

// ── Backpressure ──────────────────────────────────────────────────

func TestFullQueueRejects(t *testing.T) {
	q := NewMemQueue(2, 3, time.Minute)
	if err := q.Enqueue(&Job{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&Job{ID: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&Job{ID: "c"}); !errors.Is(err, ErrFull) {
		t.Errorf("คิวเต็ม = %v, ต้องการ ErrFull", err)
	}
	// งานที่ inflight ยังนับเป็นความจุ — ไม่งั้นจะรับงานเกินที่ระบบรับไหว
	j, _ := q.Dequeue(t.Context())
	if err := q.Enqueue(&Job{ID: "d"}); !errors.Is(err, ErrFull) {
		t.Errorf("inflight ต้องนับเป็นความจุ: %v", err)
	}
	q.Ack(j.ID)
	if err := q.Enqueue(&Job{ID: "e"}); err != nil {
		t.Errorf("หลัง Ack ต้องมีที่ว่าง: %v", err)
	}
}

// ── ความถูกต้องของ invariant ───────────────────────────────────────

// ID ซ้ำเคยทำให้ inflight[id] ถูกเขียนทับ → I2 พัง → Stats รายงานผิด,
// capacity นับผิด, และงานตัวแรกกลับมาถูกทำซ้ำหลัง lease หมดทั้งที่ ack ไปแล้ว
func TestDuplicateIDRejected(t *testing.T) {
	q := NewMemQueue(100, 3, time.Minute)
	if err := q.Enqueue(&Job{ID: "same"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&Job{ID: "same"}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("ID ซ้ำ = %v, ต้องการ ErrDuplicateID", err)
	}

	// ID ต้องถูกจองไว้ตลอดวงจรชีวิต รวมถึงตอน inflight
	j, _ := q.Dequeue(t.Context())
	if err := q.Enqueue(&Job{ID: "same"}); !errors.Is(err, ErrDuplicateID) {
		t.Errorf("ID ที่ inflight อยู่ = %v, ต้องการ ErrDuplicateID", err)
	}

	// Ack แล้ว ID ต้องว่างให้ใช้ซ้ำได้
	if err := q.Ack(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&Job{ID: "same"}); err != nil {
		t.Errorf("หลัง Ack ต้อง enqueue ID เดิมได้: %v", err)
	}
}

func TestDuplicateIDFreedAfterDLQ(t *testing.T) {
	q := NewMemQueue(100, 1, time.Minute) // maxAttempt=1 → nack ครั้งเดียวลง DLQ
	q.Enqueue(&Job{ID: "poison"})
	j, _ := q.Dequeue(t.Context())
	q.Nack(j.ID, 0, errors.New("เทส"))

	if s := q.Stats(); s.Dead != 1 {
		t.Fatalf("ต้องอยู่ DLQ: %+v", s)
	}
	// แก้บั๊กแล้ว re-enqueue ID เดิมได้ (DLQ = จบวงจร)
	if err := q.Enqueue(&Job{ID: "poison"}); err != nil {
		t.Errorf("หลังลง DLQ ต้อง enqueue ID เดิมได้: %v", err)
	}
}

// Extend ต้องกันงานยาวไม่ให้ถูกส่งซ้ำ และต้องบอกได้เมื่อเสีย lease ไปแล้ว
func TestExtendKeepsLongJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const vt = 30 * time.Second
		q := NewMemQueue(10, 5, vt)
		q.Enqueue(&Job{ID: "long"})
		j, _ := q.Dequeue(t.Context())

		// งานใช้ 75 วิ > vt — heartbeat ทุก vt/3 ต้องกันการส่งซ้ำได้
		for range 8 {
			time.Sleep(vt / 3)
			if err := q.Extend(j.ID, vt); err != nil {
				t.Fatalf("Extend = %v — งานถูกแย่งไปแล้ว", err)
			}
		}
		if s := q.Stats(); s.Inflight != 1 || s.Ready != 0 {
			t.Errorf("งานต้องยังอยู่กับ worker เดิม: %+v", s)
		}
		if err := q.Ack("long"); err != nil {
			t.Errorf("Ack = %v", err)
		}

		// ไม่มี Extend → หลัง vt งานหลุดมือ และต้องนับเป็น AckTooLate
		q.Enqueue(&Job{ID: "lost"})
		k, _ := q.Dequeue(t.Context())
		time.Sleep(vt + time.Second)
		if err := q.Extend(k.ID, vt); !errors.Is(err, ErrNotInflight) {
			t.Errorf("Extend หลัง lease หมด = %v, ต้องการ ErrNotInflight", err)
		}
		if got := q.Stats().AckTooLate; got != 1 {
			t.Errorf("AckTooLate = %d, ต้องการ 1", got)
		}
	})
}

// ── การปลดบล็อก Dequeue ────────────────────────────────────────────

func TestDequeueUnblocks(t *testing.T) {
	t.Run("ctx cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 3, time.Minute)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()
			if _, err := q.Dequeue(ctx); !errors.Is(err, context.Canceled) {
				t.Errorf("= %v, ต้องการ context.Canceled", err)
			}
		})
	})

	t.Run("close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 3, time.Minute)
			go func() {
				time.Sleep(time.Second)
				q.Close()
			}()
			if _, err := q.Dequeue(context.Background()); !errors.Is(err, ErrClosed) {
				t.Errorf("= %v, ต้องการ ErrClosed", err)
			}
		})
	})
}

// ── ขอบเขตความเป็นเจ้าของ: คิวห้ามแชร์ *Job กับ worker ────────────────

// เดิม Dequeue คืน pointer ตัวที่คิวยังถืออยู่ → พอ lease หมดแล้วแจกซ้ำ
// คิวเขียน j.Attempt++ ใต้ล็อก ขณะ worker เก่าอ่านนอกล็อก = data race
func TestDequeueReturnsIsolatedCopy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const vt = 10 * time.Second
		q := NewMemQueue(10, 5, vt)
		q.Enqueue(&Job{ID: "j"})

		a, err := q.Dequeue(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(vt + time.Second) // lease หมด → งานกลับเข้า ready
		b, err := q.Dequeue(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		if a == b {
			t.Fatal("Dequeue คืน pointer ตัวเดียวกันสองครั้ง — คิวแชร์ state กับ worker")
		}
		// worker เก่าต้องเห็นค่าที่ตัวเองได้รับตอนแรกเสมอ ไม่ใช่ค่าที่คิวแก้ทีหลัง
		if a.Attempt != 1 || b.Attempt != 2 {
			t.Errorf("a.Attempt = %d (ต้อง 1), b.Attempt = %d (ต้อง 2)", a.Attempt, b.Attempt)
		}
	})
}

// เส้นที่ race detector ต้องเงียบ: lease หมดกลางคัน handler ผ่าน RunPool ล้วน
// (RunPool อ่าน j.Attempt เองใน Backoff(j.Attempt) — ไม่ใช่ความผิดผู้ใช้)
func TestNoRaceWhenLeaseExpiresMidHandler(t *testing.T) {
	q := NewMemQueue(50, 1000, 5*time.Millisecond) // visibility < เวลาทำงาน โดยตั้งใจ
	for i := range 20 {
		if err := q.Enqueue(&Job{ID: fmt.Sprintf("j%02d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	RunPool(ctx, q, 8, time.Second, func(ctx context.Context, j *Job) error {
		time.Sleep(20 * time.Millisecond)
		_ = j.Attempt // handler อ่านฟิลด์ตามปกติ — ต้องปลอดภัยเสมอ
		return errors.New("boom")
	})
}

// DLQ ต้อง replay ได้ตรง ๆ: ส่งตัวที่ได้จาก Dead() กลับเข้า Enqueue
func TestDeadJobsReplayCleanly(t *testing.T) {
	q := NewMemQueue(10, 2, time.Minute)
	q.Enqueue(&Job{ID: "p"})
	for range 2 {
		j, _ := q.Dequeue(t.Context())
		q.Nack(j.ID, 0, errors.New("boom"))
	}

	dead := q.Dead()
	if len(dead) != 1 || dead[0].Attempt != 2 {
		t.Fatalf("DLQ = %+v", dead)
	}
	if err := q.Enqueue(dead[0]); err != nil { // replay หลังแก้บั๊กปลายทาง
		t.Fatalf("replay = %v", err)
	}
	// ต้องได้ retry budget ใหม่เต็ม ไม่ใช่เด้งกลับ DLQ ทันทีที่ล้มครั้งแรก
	j, _ := q.Dequeue(t.Context())
	if j.Attempt != 1 {
		t.Errorf("replay แล้ว Attempt = %d, ต้องเริ่มนับใหม่ที่ 1", j.Attempt)
	}
	// สำเนาใน Dead() ต้องไม่ใช่ตัวเดียวกับที่คิวกำลังเขียน
	if q.Dead()[0].Attempt != 2 {
		t.Errorf("Dead() ถูกคิวเขียนทับ = %d", q.Dead()[0].Attempt)
	}
}

// ── Shutdown ต้องไม่กินโควตา retry ของงานที่ยังค้างคิว ─────────────────

// เดิม Dequeue เช็ค ready ก่อนเช็ค ctx → หลัง SIGTERM worker กวาด ready
// ทั้งกองมา nack ทิ้ง; deploy 5 ครั้งกับ maxAttempt=5 = งานลง DLQ ทั้งที่ไม่เคยรัน
func TestCancelledCtxTakesNoJob(t *testing.T) {
	const n = 20
	q := NewMemQueue(100, 5, time.Minute)
	for i := range n {
		if err := q.Enqueue(&Job{ID: fmt.Sprintf("j%02d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := q.Dequeue(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dequeue หลัง cancel = %v, ต้องการ context.Canceled ทั้งที่คิวมีงาน", err)
	}

	var handled atomic.Int64
	RunPool(ctx, q, 4, time.Second, func(context.Context, *Job) error {
		handled.Add(1)
		return nil
	})
	if got := handled.Load(); got != 0 {
		t.Errorf("pool หยิบงานไป %d ชิ้นทั้งที่ ctx ยกเลิกแล้ว", got)
	}
	if s := q.Stats(); s.Ready != n || s.Delayed != 0 {
		t.Errorf("งานต้องอยู่ครบใน ready ไม่ถูกเผา Attempt: %+v", s)
	}
}

// ── Validation ที่ขอบเขตความเชื่อถือ ────────────────────────────────

func TestInvalidConfigPanics(t *testing.T) {
	for _, c := range []struct {
		name                 string
		capacity, maxAttempt int
		visibility           time.Duration
	}{
		{"capacity=0", 0, 3, time.Minute},
		{"maxAttempt=0", 10, 0, time.Minute},
		{"visibility=0", 10, 3, 0}, // ← ทุกงานหมด lease ทันทีที่ถูกหยิบ
		{"visibility ติดลบ", 10, 3, -time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("ต้อง panic ตอนบูต ไม่ใช่พังเงียบ ๆ ตอนรัน")
				}
			}()
			NewMemQueue(c.capacity, c.maxAttempt, c.visibility)
		})
	}
}

func TestEmptyIDRejected(t *testing.T) {
	q := NewMemQueue(10, 3, time.Minute)
	if err := q.Enqueue(&Job{}); !errors.Is(err, ErrEmptyID) {
		t.Errorf("ID ว่าง = %v, ต้องการ ErrEmptyID (ไม่ใช่ ErrDuplicateID ตอนตัวที่สอง)", err)
	}
}

func TestExtendRejectsNonPositive(t *testing.T) {
	q := NewMemQueue(10, 3, time.Minute)
	q.Enqueue(&Job{ID: "j"})
	j, _ := q.Dequeue(t.Context())
	if err := q.Extend(j.ID, 0); err == nil {
		t.Error("Extend(0) ต้องคืน error — ไม่งั้นงานถูกแจกซ้ำเงียบ ๆ ทั้งที่ worker ยังทำอยู่")
	}
	if s := q.Stats(); s.Inflight != 1 {
		t.Errorf("Extend ที่ผิดต้องไม่กระทบ lease เดิม: %+v", s)
	}
}

// ── End-to-end: pool + panic guard + retry + graceful shutdown ──────

func TestPoolEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n, workers = 200, 8
		q := NewMemQueue(1000, 5, 2*time.Second)
		for i := range n {
			if err := q.Enqueue(&Job{ID: fmt.Sprintf("job-%03d", i)}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}
		q.Enqueue(&Job{ID: "boom", Payload: []byte("panic")}) // งานที่ panic ทุกครั้ง

		var mu sync.Mutex
		done, flaky := map[string]int{}, map[string]int{}
		allDone := make(chan struct{})
		closeOnce := sync.OnceFunc(func() { close(allDone) })

		ctx, cancel := context.WithCancel(context.Background())
		pool := make(chan struct{})
		go func() {
			defer close(pool)
			RunPool(ctx, q, workers, time.Second, func(ctx context.Context, j *Job) error {
				if string(j.Payload) == "panic" {
					panic("ระเบิดในงาน") // ต้องไม่ล้มทั้ง pool
				}
				mu.Lock()
				defer mu.Unlock()
				if j.Attempt == 1 && strings.HasSuffix(j.ID, "3") {
					flaky[j.ID]++ // 20 งานล้มรอบแรก เพื่อพิสูจน์ว่า retry แล้วสำเร็จจริง
					return errors.New("ล้มชั่วคราว")
				}
				done[j.ID]++
				if len(done) == n {
					closeOnce()
				}
				return nil
			})
		}()

		select {
		case <-allDone:
		case <-time.After(5 * time.Minute):
			t.Fatalf("งานไม่จบ: %+v", q.Stats())
		}

		// boom วน retry จนครบ 5 ครั้งแล้วต้องตกลง DLQ ไม่วนตลอดกาล
		deadline := time.Now().Add(5 * time.Minute)
		for q.Stats().Dead == 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}

		cancel()
		<-pool // graceful: worker ทุกตัวต้องออกเมื่อ ctx ถูกยกเลิก

		mu.Lock()
		defer mu.Unlock()
		if len(done) != n {
			t.Errorf("สำเร็จ %d/%d", len(done), n)
		}
		if len(flaky) != 20 {
			t.Errorf("งาน flaky = %d, ต้องการ 20", len(flaky))
		}
		for id, c := range done {
			if c != 1 {
				t.Errorf("%s ทำ %d ครั้ง — ไม่ควรซ้ำเมื่อ handler เร็วกว่า visibility", id, c)
			}
		}
		if done["boom"] != 0 {
			t.Error("งานที่ panic ต้องไม่ถูกนับว่าสำเร็จ")
		}
		s := q.Stats()
		if s.Dead != 1 || q.Dead()[0].ID != "boom" {
			t.Errorf("boom ต้องอยู่ DLQ: %+v", s)
		}
	})
}
