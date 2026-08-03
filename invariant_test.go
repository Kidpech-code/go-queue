package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// เทสตามตัวอย่าง (example-based) พิสูจน์ได้แค่เส้นทางที่คนเขียนเทสนึกออก
// ไฟล์นี้พิสูจน์ **คุณสมบัติ** ที่ต้องจริงทุกลำดับเหตุการณ์ โดยไม่ต้องนึกลำดับนั้นออกเอง:
//
//	checkInvariants  — I1/I2/I2b/I6 + โครงสร้าง heap; เรียกหลัง **ทุก** operation
//	TestModelRandomOps — สุ่มลำดับ operation หลายพันครั้ง เทียบกับ conservation law
//	FuzzQueueOps     — ให้ fuzzer หาลำดับที่ทำ invariant พังแทนเรา
//	TestConcurrentStress — ยิงพร้อมกันจริง ๆ ใต้ -race
//
// เหตุผลที่ต้องมี: บั๊กของคิวคือบั๊กที่เกิดจาก "ลำดับ" ไม่ใช่ "ค่า"
// heap.Remove ด้วย index ที่ค้าง จะลบงานคนอื่นทิ้ง **เงียบ ๆ** — ไม่มี error, ไม่มี panic,
// มีแค่งานที่หายไปหนึ่งชิ้นที่ไม่มีใครสังเกตจนกว่าลูกค้าจะโทรมา

// ── ตัวตรวจ invariant ─────────────────────────────────────────────────

// checkInvariants ยืนยันทุกข้อใน README §2.4 ที่ตรวจได้จากภายใน
// ราคา O(n) — ตั้งใจให้เรียกจากเทสเท่านั้น ไม่ใช่โปรดักชัน
func checkInvariants(tb testing.TB, q *MemQueue, when string) {
	tb.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()

	fail := func(format string, a ...any) {
		tb.Helper()
		tb.Fatalf("invariant พังหลัง %s: "+format, append([]any{when}, a...)...)
	}

	// โครงสร้าง heap: index ต้องตรงตำแหน่งจริง ไม่งั้น heap.Remove/Fix จะแตะงานผิดตัว
	heaps := map[string]*jobHeap{"ready": &q.ready, "delayed": &q.delayed, "leases": &q.leases}
	for name, h := range heaps {
		for i, j := range h.jobs {
			if j == nil {
				fail("%s[%d] = nil", name, i)
				continue // fail เรียก Fatalf (ไม่คืนค่า) แต่ตัววิเคราะห์ static ไม่รู้
			}
			if j.index != i {
				fail("%s[%d].index = %d (I1: index ต้องชี้ตำแหน่งจริง ไม่งั้น heap.Remove ลบผิดตัว)", name, i, j.index)
			}
			if i > 0 && h.less(h.jobs[i], h.jobs[(i-1)/2]) {
				fail("%s: heap order พังที่ %d (ลูกน้อยกว่าพ่อ) — Fix/Swap ไม่ถูกเรียก", name, i)
			}
		}
	}

	// I1: งาน 1 ชิ้นอยู่ได้ที่เดียวเท่านั้น — ตรวจด้วย pointer identity ไม่ใช่ ID
	seen := map[*Job]string{}
	for name, h := range heaps {
		for _, j := range h.jobs {
			if prev, dup := seen[j]; dup {
				fail("I1: งาน %q อยู่ทั้งใน %s และ %s พร้อมกัน", j.ID, prev, name)
			}
			seen[j] = name
		}
	}
	for _, j := range q.dead {
		if prev, dup := seen[j]; dup {
			fail("I1: งาน %q อยู่ทั้งใน %s และ dead พร้อมกัน", j.ID, prev)
		}
		if j.index != -1 {
			fail("I1: งานใน DLQ %q มี index = %d, ต้องเป็น -1 (ไม่ได้อยู่ใน heap ไหนแล้ว)", j.ID, j.index)
		}
		seen[j] = "dead"
	}

	// I2: id ∈ inflight ⟺ job ∈ leases
	if len(q.inflight) != q.leases.Len() {
		fail("I2: |inflight| = %d แต่ |leases| = %d", len(q.inflight), q.leases.Len())
	}
	inLeases := map[*Job]bool{}
	for _, j := range q.leases.jobs {
		inLeases[j] = true
	}
	for id, j := range q.inflight {
		if !inLeases[j] {
			fail("I2: %q อยู่ใน inflight แต่ไม่อยู่ใน leases — lease หมดแล้วไม่มีใครกู้ (งานค้างตลอดกาล)", id)
		}
		if j.ID != id {
			fail("I2: inflight[%q].ID = %q", id, j.ID)
		}
	}

	// I2b: ids = ready ∪ delayed ∪ inflight พอดี ไม่ขาดไม่เกิน
	live := map[string]bool{}
	for _, h := range []*jobHeap{&q.ready, &q.delayed} {
		for _, j := range h.jobs {
			if live[j.ID] {
				fail("I2b: ID %q ซ้ำในหมู่งานที่ยังไม่จบ", j.ID)
			}
			live[j.ID] = true
		}
	}
	for id := range q.inflight {
		if live[id] {
			fail("I2b: ID %q ซ้ำในหมู่งานที่ยังไม่จบ", id)
		}
		live[id] = true
	}
	for id := range live {
		if _, ok := q.ids[id]; !ok {
			fail("I2b: %q อยู่ในคิวแต่ไม่อยู่ใน ids — enqueue ID นี้ซ้ำได้ทั้งที่ยังทำอยู่", id)
		}
	}
	for id := range q.ids {
		if !live[id] {
			fail("I2b: %q อยู่ใน ids แต่ไม่อยู่ที่ไหนเลย — ID รั่ว, enqueue ใหม่ไม่ได้ตลอดกาล", id)
		}
	}

	// I6: capacity ต้องนับ inflight ด้วย ไม่งั้นรับงานเกินที่ระบบทำไหว
	if got, want := q.sizeLocked(), q.ready.Len()+q.delayed.Len()+len(q.inflight); got != want {
		fail("I6: sizeLocked = %d, ต้องการ %d (ready+delayed+inflight)", got, want)
	}
	if q.sizeLocked() > q.capacity {
		fail("I6: มีงาน %d ชิ้น เกิน capacity %d", q.sizeLocked(), q.capacity)
	}

	// I4: enqueued ต้องถูกตั้งเสมอ ไม่งั้น OldestReady โกหก
	for _, j := range q.ready.jobs {
		if j.enqueued.IsZero() {
			fail("I4: %q ไม่มีเวลาเข้าคิว → OldestReady คำนวณผิด", j.ID)
		}
	}
}

// ── model-based: สุ่มลำดับ operation แล้วตรวจกฎอนุรักษ์ ────────────────

// model นับงานแบบ "บัญชีคู่": ทุกงานที่เข้ามาต้องอยู่ที่ใดที่หนึ่งเสมอ ห้ามหายไปเฉย ๆ
//
//	enqueued = acked + taken + dead + dropped + (ready + delayed + inflight)
//
// (taken = ออกจากระบบทาง TakeDead — replay กลับเข้ามานับเป็น enqueued รอบใหม่)
// กฎข้อนี้จับบั๊กที่ invariant เชิงโครงสร้างจับไม่ได้: งานที่ถูก heap.Remove
// ด้วย index ผิดจะหายไปทั้งที่ทุกโครงสร้างยัง "ถูกต้อง" ในตัวมันเอง
type model struct{ enqueued, acked, taken int }

func (m *model) check(tb testing.TB, q *MemQueue, when string) {
	tb.Helper()
	s := q.Stats()
	got := m.acked + m.taken + s.Dead + int(s.DeadDropped) + s.Ready + s.Delayed + s.Inflight
	if got != m.enqueued {
		tb.Fatalf("กฎอนุรักษ์พังหลัง %s: enqueue ไป %d แต่นับได้ %d "+
			"(acked=%d taken=%d dead=%d dropped=%d ready=%d delayed=%d inflight=%d) — มีงานหายไปจากระบบ",
			when, m.enqueued, got, m.acked, m.taken, s.Dead, s.DeadDropped, s.Ready, s.Delayed, s.Inflight)
	}
}

// driver รัน operation ตามลำดับที่กำหนด แล้วตรวจ invariant + กฎอนุรักษ์ทุกก้าว
// ใช้ร่วมกันระหว่างเทสสุ่ม (seed คงที่ → reproduce ได้) กับ fuzz (ลำดับมาจาก fuzzer)
func driveOps(tb testing.TB, q *MemQueue, ops []byte, sleep func(time.Duration)) {
	tb.Helper()
	m := &model{}
	held := []string{} // ID ที่เคย dequeue มา — รวมตัวที่ lease หมดแล้ว เพื่อยิง error path ด้วย
	next := 0

	pick := func(b byte) string {
		if len(held) == 0 {
			return "ไม่มีตัวนี้"
		}
		return held[int(b)%len(held)]
	}

	for i := 0; i+1 < len(ops); i += 2 {
		op, arg := ops[i]%8, ops[i+1]
		what := fmt.Sprintf("op[%d]=%d(%d)", i/2, op, arg)

		switch op {
		case 0: // Enqueue — ID ใหม่เสมอ; บางตัวเป็นงาน delayed
			j := &Job{ID: fmt.Sprintf("j%04d", next), Priority: int(arg % 5)}
			if arg%3 == 0 {
				j.RunAt = time.Now().Add(time.Duration(arg%50) * time.Millisecond)
			}
			switch err := q.Enqueue(j); {
			case err == nil:
				m.enqueued++
				next++
			case errors.Is(err, ErrFull): // backpressure = พฤติกรรมที่ถูกต้อง ไม่ใช่ความล้มเหลว
			default:
				tb.Fatalf("%s: Enqueue = %v", what, err)
			}

		case 1: // Dequeue — เช็ก Ready ก่อนเพื่อไม่ให้บล็อก (goroutine เดียว จึงไม่มีใครมาแย่ง)
			if q.Stats().Ready == 0 {
				continue
			}
			j, err := q.Dequeue(context.Background())
			if err != nil {
				tb.Fatalf("%s: Dequeue = %v ทั้งที่ Ready > 0", what, err)
			}
			if j.Attempt < 1 {
				tb.Fatalf("%s: I3 พัง — Attempt = %d หลัง Dequeue", what, j.Attempt)
			}
			held = append(held, j.ID)

		case 2: // Ack — ทั้งตัวที่ยังถืออยู่และตัวที่ lease หมดไปแล้ว
			if err := q.Ack(pick(arg)); err == nil {
				m.acked++
			} else if !errors.Is(err, ErrNotInflight) {
				tb.Fatalf("%s: Ack = %v", what, err)
			}

		case 3: // Nack พร้อม backoff สุ่ม
			err := q.Nack(pick(arg), time.Duration(arg%30)*time.Millisecond, errors.New("boom"))
			if err != nil && !errors.Is(err, ErrNotInflight) {
				tb.Fatalf("%s: Nack = %v", what, err)
			}

		case 4: // Extend
			err := q.Extend(pick(arg), time.Duration(1+arg%40)*time.Millisecond)
			if err != nil && !errors.Is(err, ErrNotInflight) {
				tb.Fatalf("%s: Extend = %v", what, err)
			}

		case 5: // เวลาผ่านไป → lease หมด / งาน delayed ถึงเวลา
			sleep(time.Duration(arg%40) * time.Millisecond)

		case 6: // Dead() ต้องคืนสำเนา — แก้ของที่ได้มาแล้วคิวต้องไม่กระเทือน
			for _, d := range q.Dead() {
				d.Attempt, d.LastErr = -999, "แก้จากข้างนอก"
			}

		case 7: // TakeDead + replay ครึ่งหนึ่ง — เส้นทางที่ §6.8 แนะนำ
			taken := q.TakeDead(int(arg%3) - 1) // -1..1: ยิงทั้งขอบติดลบ ศูนย์ และดึงจริง
			m.taken += len(taken)
			if arg%2 == 0 {
				for _, d := range taken {
					switch err := q.Enqueue(d); {
					case err == nil:
						m.enqueued++ // เข้าระบบรอบใหม่ — ฝั่ง taken ไม่ลด (ดูสมการของ model)
					case errors.Is(err, ErrFull): // งานหายแบบตั้งใจ — ยังอยู่ในบัญชี taken
					default:
						tb.Fatalf("%s: replay Enqueue = %v", what, err)
					}
				}
			}
		}

		checkInvariants(tb, q, what)
		m.check(tb, q, what)
	}
}

// TestModelRandomOps: 20 seed × 400 operation — เวลาปลอมจึงจบใน ~0 วินาที
// seed คงที่: เทสแดงแล้ว reproduce ได้ทันที ไม่ใช่ "ลองรันใหม่ดู"
func TestModelRandomOps(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := rand.New(rand.NewPCG(seed, 0xC0FFEE))
				ops := make([]byte, 800)
				for i := range ops {
					ops[i] = byte(r.UintN(256))
				}
				// capacity เล็กโดยตั้งใจ → เจอ ErrFull และเส้นทาง DLQ ล้นบ่อย ๆ
				q := NewMemQueue(8, 3, 20*time.Millisecond)
				driveOps(t, q, ops, func(d time.Duration) { time.Sleep(d) })
			})
		})
	}
}

// FuzzQueueOps: ให้ fuzzer หาลำดับที่เราไม่ได้นึกถึง
//
//	go test -run=Fuzz -fuzz=FuzzQueueOps -fuzztime=60s
//
// ใช้เวลาจริง (ไม่ใช่ synctest) เพราะ fuzz ต้องรันซ้ำเร็ว ๆ หลายแสนรอบ
// assert ทั้งหมดเป็นเชิงโครงสร้างจึงไม่ขึ้นกับเวลา แต่ **ความยาว** ของแต่ละ exec ขึ้น:
// input ที่อัด op นอนจนรวมได้หลายร้อย ms บวก CI ที่โหลดสูง ทำให้ engine ของ go
// ยิง "context deadline exceeded" ตอน fuzztime หมดพอดี (เจอจริง: CI แดงทั้งที่ไม่มี crasher)
// → จำกัดงบนอนรวมต่อหนึ่ง input ให้ exec จบเร็วเสมอ
func FuzzQueueOps(f *testing.F) {
	f.Add([]byte{0, 1, 1, 0, 2, 0})                    // enqueue → dequeue → ack
	f.Add([]byte{0, 0, 1, 0, 3, 0, 5, 30, 1, 0, 2, 0}) // nack → รอ → dequeue ซ้ำ
	f.Add([]byte{0, 0, 1, 0, 5, 39, 2, 0, 4, 0})       // lease หมด แล้วค่อย ack/extend
	f.Add([]byte{0, 3, 0, 3, 0, 3, 1, 0, 1, 0, 6, 0})  // ชน capacity
	// enqueue → nack จนครบโควตา (maxAttempt=3) → ลง DLQ → TakeDead + replay
	f.Add([]byte{0, 0, 1, 0, 3, 0, 1, 0, 3, 0, 1, 0, 3, 0, 7, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 400 { // จำกัดขนาดเพื่อให้แต่ละรอบเร็ว — fuzzer ชอบ input สั้นอยู่แล้ว
			ops = ops[:400]
		}
		q := NewMemQueue(8, 3, 2*time.Millisecond)
		budget := 50 * time.Millisecond // เกิน visibility (2ms) หลายเท่า — เส้นทาง lease หมดยังถูกยิงครบ
		driveOps(t, q, ops, func(d time.Duration) {
			d = min(d/20, budget)
			budget -= d
			time.Sleep(d)
		})
	})
}

// ── ยิงพร้อมกันจริง ๆ: -race + invariant ตอนจบ ─────────────────────────

// เทสตามตัวอย่างทั้งหมดเป็น single-goroutine — ไม่มีตัวไหนบังคับให้ Ack/Nack/Extend/
// Enqueue/Stats/Close ทำงานทับกันจริง เทสนี้คือเส้นที่ race detector ต้องเงียบ
func TestConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: ข้ามใน -short")
	}
	const (
		producers = 4
		workers   = 8
		perProd   = 400
	)
	q := NewMemQueue(200, 4, 2*time.Millisecond) // visibility สั้นมาก → lease หมดกลางคันตลอด
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var acked, nacked, produced atomic.Int64

	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perProd {
				err := q.Enqueue(&Job{ID: fmt.Sprintf("p%d-%04d", p, i), Priority: i % 3})
				if err == nil {
					produced.Add(1)
					continue
				}
				if !errors.Is(err, ErrFull) {
					t.Errorf("Enqueue = %v", err)
					return
				}
				time.Sleep(time.Millisecond) // คิวเต็ม = backpressure ทำงาน; ถอยแล้วลองใหม่
			}
		}()
	}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				j, err := q.Dequeue(ctx)
				if err != nil {
					return
				}
				_ = j.Attempt // อ่านฟิลด์นอกล็อก — สำเนาต้องแยกขาดจริง ไม่งั้น -race ร้อง
				switch j.seq % 4 {
				case 0:
					if err := q.Nack(j.ID, time.Millisecond, errors.New("boom")); err == nil {
						nacked.Add(1)
					}
				case 1:
					q.Extend(j.ID, 5*time.Millisecond)
					fallthrough
				default:
					if err := q.Ack(j.ID); err == nil {
						acked.Add(1)
					}
				}
			}
		}()
	}

	// ผู้สังเกตการณ์: Stats/Dead มี side effect (promote) จึงต้องยิงทับคนอื่นด้วย
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			q.Stats()
			q.Dead()
			time.Sleep(200 * time.Microsecond)
		}
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()
	q.Close()
	wg.Wait()

	checkInvariants(t, q, "stress")
	if produced.Load() == 0 || acked.Load() == 0 || nacked.Load() == 0 {
		t.Fatalf("stress ไม่ได้ทดสอบอะไรเลย: produced=%d acked=%d nacked=%d",
			produced.Load(), acked.Load(), nacked.Load())
	}
	s := q.Stats()
	if total := s.Ready + s.Delayed + s.Inflight + s.Dead + int(s.DeadDropped) + int(acked.Load()); total != int(produced.Load()) {
		t.Errorf("งานหายระหว่างยิงพร้อมกัน: enqueue %d, นับได้ %d (%+v acked=%d)",
			produced.Load(), total, s, acked.Load())
	}
}

// Close ระหว่างที่ worker กำลังบล็อกรออยู่ต้องปลุกทุกตัว ไม่ค้างแม้แต่ตัวเดียว
func TestCloseWakesEveryWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const waiters = 64
		q := NewMemQueue(10, 3, time.Minute)
		var done atomic.Int64
		for range waiters {
			go func() {
				if _, err := q.Dequeue(context.Background()); errors.Is(err, ErrClosed) {
					done.Add(1)
				}
			}()
		}
		synctest.Wait() // ทุกตัวบล็อกอยู่จริงแล้ว

		q.Close()
		synctest.Wait()
		if got := done.Load(); got != waiters {
			t.Errorf("Close ปลุกได้ %d/%d — waiter ที่เหลือค้างตลอดกาล (shutdown ไม่จบ)", got, waiters)
		}
	})
}

// goroutine/timer ต้องไม่รั่ว: Dequeue ที่ถูกยกเลิกและ RunPool ที่ปิดแล้ว
// ต้องคืนทรัพยากรครบ ไม่งั้นโปรเซสอายุยาวจะสะสมจนตาย
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 50 {
		q := NewMemQueue(10, 3, 10*time.Millisecond)
		q.Enqueue(&Job{ID: "j"})
		ctx, cancel := context.WithCancel(context.Background())
		RunPool(ctx, q, 4, time.Second, func(context.Context, *Job) error {
			cancel() // ยกเลิกจากในงานเอง — เส้นทาง shutdown ที่แคบที่สุด
			return nil
		})
		cancel()
		q.Close()
	}
	// รอจน "นิ่ง" ไม่ใช่รอเวลาตายตัว — sleep คงที่คือเทสที่เขียวบนเครื่องเราและแดงบน CI ที่โหลดสูง
	// (ยอมให้เกิน 2 ตัว เพราะ runtime เองก็มี goroutine ของ GC/timer ที่โผล่มาไม่แน่นอน)
	deadline := time.Now().Add(2 * time.Second)
	after := runtime.NumGoroutine()
	for after > before+2 && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	if after > before+2 {
		t.Errorf("goroutine รั่ว: ก่อน %d หลัง %d (รอจนนิ่ง 2 วิแล้ว)", before, after)
	}
}
