package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"weak"
)

// ไฟล์นี้เทส "สเปก" ไม่ใช่ "โค้ด" — ทุกเทสในนี้ผูกกับข้อความใน README ที่สัญญาไว้
// เขียนขึ้นเพราะ statement coverage 99% แต่ mutation score แค่ 71%:
// เทสรันผ่านทุกบรรทัด แต่ไม่ล้มเวลาบรรทัดนั้นผิด (ดู tools/mutate)
//
// ทุกเทสมีคอมเมนต์ว่า "ฆ่า mutant ตัวไหน" — ถ้าลบเทสทิ้ง mutation score จะตกทันที
// นั่นคือนิยามของเทสที่มีประโยชน์

// ── §2.3 ตารางการเปลี่ยนสถานะ — ทุกช่องต้องถูกยืนยัน ────────────────────

// state คือสถานะตั้งต้นที่เทสต้องสร้างก่อนยิง event
type state string

const (
	stNone     state = "ไม่มี"
	stDelayed  state = "DELAYED"
	stReady    state = "READY"
	stInflight state = "INFLIGHT"
	stDLQ      state = "DLQ"
)

const specID = "j"

// setup สร้างคิวที่มีงาน specID อยู่ในสถานะที่ต้องการพอดี
// ต้องอยู่ใน synctest bubble เพราะ stDLQ/stDelayed ใช้เวลาปลอม
func setup(t *testing.T, st state) *MemQueue {
	t.Helper()
	q := NewMemQueue(10, 2, 30*time.Second)
	switch st {
	case stNone:
	case stDelayed:
		mustEnqueue(t, q, &Job{ID: specID, RunAt: time.Now().Add(time.Hour)})
	case stReady:
		mustEnqueue(t, q, &Job{ID: specID})
	case stInflight:
		mustEnqueue(t, q, &Job{ID: specID})
		mustDequeue(t, q)
	case stDLQ:
		mustEnqueue(t, q, &Job{ID: specID})
		for range 2 { // maxAttempt=2
			j := mustDequeue(t, q)
			if err := q.Nack(j.ID, 0, errors.New("boom")); err != nil {
				t.Fatalf("setup Nack: %v", err)
			}
		}
		if s := q.Stats(); s.Dead != 1 {
			t.Fatalf("setup %s: %+v", st, s)
		}
	}
	return q
}

// TestSpecStateMatrix เดินทุกช่องของตาราง §2.3 — ช่องที่ "เป็นไปไม่ได้" คือที่ที่บั๊กชอบซ่อน
func TestSpecStateMatrix(t *testing.T) {
	// Ack/Nack ต้องคืน ErrNotInflight ทุกสถานะยกเว้น INFLIGHT
	for _, st := range []state{stNone, stDelayed, stReady, stDLQ} {
		t.Run(string(st)+"/Ack→ErrNotInflight", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := setup(t, st)
				if err := q.Ack(specID); !errors.Is(err, ErrNotInflight) {
					t.Errorf("Ack = %v, ต้องการ ErrNotInflight", err)
				}
			})
		})
		t.Run(string(st)+"/Nack→ErrNotInflight", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := setup(t, st)
				if err := q.Nack(specID, 0, errors.New("x")); !errors.Is(err, ErrNotInflight) {
					t.Errorf("Nack = %v, ต้องการ ErrNotInflight", err)
				}
			})
		})
	}

	// Enqueue ซ้ำ ID: ถูกปฏิเสธตราบใดที่งานยังไม่จบ; DLQ = จบวงจร → ใช้ ID ซ้ำได้
	for _, c := range []struct {
		st   state
		want error
	}{
		{stNone, nil}, {stDelayed, ErrDuplicateID}, {stReady, ErrDuplicateID},
		{stInflight, ErrDuplicateID}, {stDLQ, nil},
	} {
		t.Run(string(c.st)+"/Enqueue", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := setup(t, c.st)
				if err := q.Enqueue(&Job{ID: specID}); !errors.Is(err, c.want) {
					t.Errorf("Enqueue = %v, ต้องการ %v", err, c.want)
				}
			})
		})
	}

	// Dequeue: READY เท่านั้นที่ได้งาน — สถานะอื่นต้องบล็อก (งานถูกซ่อน/ยังไม่ถึงเวลา/จบแล้ว)
	for _, c := range []struct {
		st    state
		serve bool
	}{
		{stNone, false}, {stDelayed, false}, {stReady, true},
		{stInflight, false}, {stDLQ, false},
	} {
		t.Run(string(c.st)+"/Dequeue", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := setup(t, c.st)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				type res struct {
					j   *Job
					err error
				}
				out := make(chan res, 1)
				go func() { j, err := q.Dequeue(ctx); out <- res{j, err} }()

				synctest.Wait() // รอให้ทุก goroutine นิ่ง โดยไม่เดินนาฬิกา
				select {
				case r := <-out:
					if !c.serve {
						t.Fatalf("%s ต้องไม่ได้งาน แต่ได้ %+v (%v)", c.st, r.j, r.err)
					}
					if r.err != nil || r.j.ID != specID || r.j.Attempt != 1 {
						t.Fatalf("READY→INFLIGHT ผิด: %+v, %v (Attempt ต้อง = 1)", r.j, r.err)
					}
					if s := q.Stats(); s.Inflight != 1 || s.Ready != 0 {
						t.Errorf("หลัง Dequeue: %+v", s)
					}
				default:
					if c.serve {
						t.Fatal("READY ต้องได้งานทันที แต่บล็อก")
					}
					cancel()
					<-out // goroutine ทุกตัวต้องออกก่อน bubble จบ ไม่งั้น synctest panic
				}
			})
		})
	}

	// คอลัมน์ "เวลาผ่านไป": DELAYED→READY เมื่อถึง RunAt, INFLIGHT→READY เมื่อ lease หมด
	t.Run("DELAYED/เวลาผ่านไป→READY", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 2, 30*time.Second)
			mustEnqueue(t, q, &Job{ID: specID, RunAt: time.Now().Add(time.Minute)})
			if s := q.Stats(); s.Delayed != 1 || s.Ready != 0 {
				t.Fatalf("ก่อนถึงเวลา: %+v", s)
			}
			time.Sleep(time.Minute) // RunAt ≤ now พอดี — ขอบเขตต้องนับว่า "ถึงแล้ว"
			if s := q.Stats(); s.Ready != 1 || s.Delayed != 0 {
				t.Errorf("ที่ RunAt พอดีต้อง READY แล้ว: %+v", s)
			}
		})
	})
	t.Run("INFLIGHT/เวลาผ่านไป→READY", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := setup(t, stInflight)
			time.Sleep(30 * time.Second) // leaseUntil ≤ now พอดี
			if s := q.Stats(); s.Ready != 1 || s.Inflight != 0 {
				t.Errorf("ที่ lease หมดพอดีต้องกลับ READY: %+v", s)
			}
		})
	})
}

// ── Nack delay: ความล้มเหลวที่ "รู้ตัว" ต้องรอ (§2.7) ────────────────────

// mutant ที่รอด: ลบ `j.RunAt = time.Now().Add(delay)` ใน retryLocked ทิ้ง
// → backoff ถูกเพิกเฉยทั้งระบบ, retry ทันที, DB ที่ล่มโดนถล่มซ้ำ
// เทสเดิมทั้งหมด Nack ด้วย delay=0 จึงมองไม่เห็น
func TestNackDelayIsHonored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 5 * time.Second
		q := NewMemQueue(10, 5, time.Minute)
		mustEnqueue(t, q, &Job{ID: "j"})
		j := mustDequeue(t, q)

		start := time.Now()
		if err := q.Nack(j.ID, delay, errors.New("DB ล่ม")); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		if s := q.Stats(); s.Delayed != 1 || s.Ready != 0 {
			t.Fatalf("nack แล้วต้องรอใน delayed ก่อน: %+v", s)
		}
		if k := mustDequeue(t, q); k.Attempt != 2 {
			t.Errorf("Attempt = %d, ต้องการ 2", k.Attempt)
		}
		if waited := time.Since(start); waited != delay {
			t.Errorf("งานกลับมาหลัง %v, ต้องการ %v พอดี", waited, delay)
		}
	})
}

// ── §6.1 OldestReady + I4: SLI ที่ควร alert ────────────────────────────

// mutant ที่รอด: ลบ `j.enqueued = time.Now()`, ลบ `s.OldestReady = age`,
// `>` → `<` (กลายเป็นหาตัวใหม่สุด), OldestReady เริ่มต้นที่ 1 แทน 0
// → dashboard โกหกว่าไม่มีงานค้าง ทั้งที่งานค้างมาสามชั่วโมง
func TestStatsOldestReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 5, time.Minute)
		if got := q.Stats().OldestReady; got != 0 {
			t.Errorf("คิวว่าง OldestReady = %v, ต้องการ 0", got)
		}

		mustEnqueue(t, q, &Job{ID: "เก่า"})
		time.Sleep(time.Hour)
		mustEnqueue(t, q, &Job{ID: "ใหม่"})
		time.Sleep(time.Minute)

		// ต้องรายงาน "ตัวที่เก่าที่สุด" ไม่ใช่ตัวใหม่สุด ไม่ใช่ค่าเฉลี่ย
		if got, want := q.Stats().OldestReady, time.Hour+time.Minute; got != want {
			t.Errorf("OldestReady = %v, ต้องการ %v (อายุของงานที่เก่าที่สุด)", got, want)
		}
	})
}

// I4: retry ต้องไม่รีเซ็ตนาฬิกา ไม่งั้นงานที่วน retry มาสามชั่วโมงจะดู "เพิ่งเข้าคิว"
func TestOldestReadySurvivesRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 5, time.Minute)
		mustEnqueue(t, q, &Job{ID: "j"})
		time.Sleep(2 * time.Hour)

		j := mustDequeue(t, q)
		if err := q.Nack(j.ID, 0, errors.New("boom")); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		if got := q.Stats().OldestReady; got != 2*time.Hour {
			t.Errorf("หลัง retry OldestReady = %v, ต้องยังเป็น 2h (I4: อายุนับจากเข้าคิวครั้งแรก)", got)
		}
	})
}

// ── I2c: ทุก method ต้อง promote ก่อนตัดสินใจ ──────────────────────────

// mutant ที่รอด: ลบ promoteLocked ออกจาก Ack / Nack / Stats
// → พฤติกรรมขึ้นกับว่ามีใครเรียก Dequeue คั่นหรือไม่ = ทดสอบไม่ได้, debug ไม่ได้
func TestPromoteBeforeEveryDecision(t *testing.T) {
	// ห้ามมี Dequeue คั่นในเทสนี้ — นั่นคือทั้งหมดของประเด็น
	ops := map[string]func(q *MemQueue) error{
		"Ack":    func(q *MemQueue) error { return q.Ack("j") },
		"Nack":   func(q *MemQueue) error { return q.Nack("j", 0, nil) },
		"Extend": func(q *MemQueue) error { return q.Extend("j", time.Minute) },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const vt = 10 * time.Second
				q := NewMemQueue(10, 5, vt)
				mustEnqueue(t, q, &Job{ID: "j"})
				mustDequeue(t, q)
				time.Sleep(vt + time.Second) // lease หมดแล้ว แต่ไม่มีใคร Dequeue มาปลุก

				if err := op(q); !errors.Is(err, ErrNotInflight) {
					t.Errorf("%s หลัง lease หมด = %v, ต้องการ ErrNotInflight", name, err)
				}
			})
		})
	}

	t.Run("Stats", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 5, 10*time.Second)
			mustEnqueue(t, q, &Job{ID: "delayed", RunAt: time.Now().Add(time.Second)})
			mustEnqueue(t, q, &Job{ID: "leased"})
			mustDequeue(t, q)
			time.Sleep(11 * time.Second)

			// Stats ต้องเห็นความจริง ณ ตอนถาม ไม่ใช่ ณ ครั้งสุดท้ายที่มีคน Dequeue
			if s := q.Stats(); s.Ready != 2 || s.Delayed != 0 || s.Inflight != 0 {
				t.Errorf("Stats = %+v, ต้องการ Ready=2 (promote ทั้ง delayed และ lease ที่หมดอายุ)", s)
			}
		})
	})
}

// ── SLI: AckTooLate ต้องนับทุกทาง ไม่ใช่แค่ Extend ─────────────────────

func TestAckTooLateCountsEveryPath(t *testing.T) {
	for name, op := range map[string]func(q *MemQueue) error{
		"Ack":    func(q *MemQueue) error { return q.Ack("j") },
		"Nack":   func(q *MemQueue) error { return q.Nack("j", 0, nil) },
		"Extend": func(q *MemQueue) error { return q.Extend("j", time.Minute) },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const vt = 10 * time.Second
				q := NewMemQueue(10, 5, vt)
				mustEnqueue(t, q, &Job{ID: "j"})
				mustDequeue(t, q)
				time.Sleep(vt + time.Second)

				_ = op(q)
				if got := q.Stats().AckTooLate; got != 1 {
					t.Errorf("%s ที่มาสาย: AckTooLate = %d, ต้องการ 1 — SLI นี้คือสัญญาณว่า visibility สั้นไป", name, got)
				}
			})
		})
	}
}

// ── Extend ต้องจัดลำดับ lease heap ใหม่ (heap.Fix) ─────────────────────

// mutant ที่รอด: ลบ heap.Fix ใน Extend
// เทสเดิมมีงาน inflight ตัวเดียว heap 1 ตัวเรียงถูกเสมอ → มองไม่เห็น
// ของจริง: งาน A ที่ต่ออายุแล้วยังนั่งอยู่หัว heap → promote เห็น A ก่อน B ที่หมดจริง
func TestExtendReordersLeaseHeap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const vt = 10 * time.Second
		q := NewMemQueue(10, 5, vt)
		mustEnqueue(t, q, &Job{ID: "a"})
		mustEnqueue(t, q, &Job{ID: "b"})
		a, b := mustDequeue(t, q), mustDequeue(t, q)
		if a.ID != "a" || b.ID != "b" {
			t.Fatalf("ลำดับ dequeue = %s, %s", a.ID, b.ID)
		}

		// a ต่ออายุยาว → b ต้องกลายเป็นตัวที่หมดอายุก่อน
		if err := q.Extend("a", time.Hour); err != nil {
			t.Fatalf("Extend: %v", err)
		}
		time.Sleep(vt + time.Second)

		if s := q.Stats(); s.Ready != 1 || s.Inflight != 1 {
			t.Fatalf("ต้องมีแค่ b ที่หลุด: %+v", s)
		}
		if j := mustDequeue(t, q); j.ID != "b" {
			t.Errorf("ตัวที่ถูกส่งซ้ำ = %s, ต้องเป็น b (a ต่ออายุไปแล้ว)", j.ID)
		}
		if err := q.Ack("a"); err != nil {
			t.Errorf("a ต้องยังอยู่กับ worker เดิม: %v", err)
		}
	})
}

// ── Dequeue ที่บล็อกอยู่ต้องถูกปลุกทุกเหตุการณ์ ─────────────────────────

// mutant ที่รอด: ลบ broadcastLocked ออกจาก Enqueue / Nack / promoteLocked,
// ลบ timer.Reset ใน Dequeue → worker หลับยาวทั้งที่มีงานรออยู่ (คิวค้างเงียบ ๆ)
// เทสเดิมรอด เพราะ enqueue ก่อน dequeue เสมอ — ไม่เคยมี waiter จริง
// prepare ทำงาน **ก่อน** waiter บล็อก, trigger ทำ **หลัง** — การแยกสองจังหวะนี้คือทั้งหมด
// ของเทส: ถ้ายิง trigger ก่อน waiter บล็อกจริง สัญญาณปลุกจากขั้นตอน setup จะกลบผลลัพธ์
// (เวอร์ชันแรกของเทสนี้ทำแบบนั้น แล้ว mutant `ลบ broadcast ใน Nack` รอดไปได้)
func TestBlockedDequeueIsWoken(t *testing.T) {
	const vt = time.Minute
	for name, c := range map[string]struct {
		prepare, trigger func(t *testing.T, q *MemQueue)
		// want คือเวลาที่ waiter **ต้อง** ได้งาน — ยืนยันแค่ "ได้งานในที่สุด" ไม่พอ:
		// timer ที่ตั้งไว้รอ lease หมดจะกลบ broadcast ที่หายไป (ตื่นช้า แต่ยังตื่น)
		// mutant `ลบ broadcast ใน Nack` รอดมาได้เพราะเหตุนี้ จนกว่าจะเทียบเวลาเป๊ะ
		want time.Duration
	}{
		"Enqueue ปลุกทันที": {
			trigger: func(t *testing.T, q *MemQueue) { mustEnqueue(t, q, &Job{ID: "j"}) },
			want:    0,
		},
		"Nack ปลุกทันที": {
			prepare: func(t *testing.T, q *MemQueue) {
				mustEnqueue(t, q, &Job{ID: "j"})
				mustDequeue(t, q)
			},
			trigger: func(t *testing.T, q *MemQueue) {
				if err := q.Nack("j", 0, errors.New("boom")); err != nil {
					t.Fatalf("Nack: %v", err)
				}
			},
			want: 0, // ★ ไม่ใช่ vt: §2.7 บอกว่า Nack มีอยู่เพื่อ "คืนงานทันที" ไม่ใช่รอ lease หมด
		},
		// ไม่มี trigger: waiter ต้องตื่นเองด้วย timer ที่ตั้งตาม nextEventLocked
		"lease หมดปลุก": {
			prepare: func(t *testing.T, q *MemQueue) {
				mustEnqueue(t, q, &Job{ID: "j"})
				mustDequeue(t, q)
			},
			want: vt,
		},
		"RunAt ถึงปลุก": {
			prepare: func(t *testing.T, q *MemQueue) {
				mustEnqueue(t, q, &Job{ID: "j", RunAt: time.Now().Add(time.Minute)})
			},
			want: time.Minute,
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q := NewMemQueue(10, 5, vt)
				if c.prepare != nil {
					c.prepare(t, q)
				}
				type res struct {
					id      string
					elapsed time.Duration
				}
				got := make(chan res, 1)
				start := time.Now()
				go func() {
					j, err := q.Dequeue(context.Background())
					if err != nil {
						got <- res{"err:" + err.Error(), time.Since(start)}
						return
					}
					got <- res{j.ID, time.Since(start)}
				}()
				synctest.Wait() // waiter บล็อกอยู่จริงแล้ว ค่อยยิง trigger

				if c.trigger != nil {
					c.trigger(t, q)
				}
				select {
				case r := <-got:
					if r.id != "j" {
						t.Errorf("waiter ได้ %q, ต้องการ j", r.id)
					}
					if r.elapsed != c.want {
						t.Errorf("waiter ตื่นหลัง %v, ต้องการ %v พอดี", r.elapsed, c.want)
					}
				case <-time.After(10 * time.Minute):
					t.Fatal("waiter ไม่ถูกปลุก — มีงานรออยู่แต่ worker หลับ")
				}
			})
		})
	}
}

// timer ตัวเดียวถูกใช้ซ้ำทั้งลูปของ Dequeue → ต้อง Reset ทุกครั้งที่วนใหม่
//
// mutant `ลบ timer.Reset(d)`: waiter ที่ **แพ้การแย่งงาน** จะกลับไปรอ channel ของ timer
// ที่ยิงไปแล้วและไม่มีวันยิงอีก = หลับตลอดกาลทั้งที่มีงานรออยู่
// ต้องมี waiter สองตัวถึงจะเห็น — ตัวเดียวไม่มีวันแพ้
func TestLosingWaiterRearmsTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 5, time.Hour)
		start := time.Now()
		mustEnqueue(t, q, &Job{ID: "1", RunAt: start.Add(1 * time.Minute)})
		mustEnqueue(t, q, &Job{ID: "2", RunAt: start.Add(2 * time.Minute)})

		got := make(chan string, 2)
		for range 2 { // ทั้งคู่ตั้ง timer ที่ +1m; พอถึงเวลา มีแค่ตัวเดียวที่ได้งาน
			go func() {
				j, err := q.Dequeue(context.Background())
				if err != nil {
					got <- "err:" + err.Error()
					return
				}
				got <- j.ID + "@" + time.Since(start).String()
			}()
		}

		first, second := <-got, <-got
		if first > second { // ให้ลำดับแน่นอนโดยไม่ต้องสน goroutine ไหนชนะ
			first, second = second, first
		}
		if first != "1@1m0s" || second != "2@2m0s" {
			t.Errorf("ได้ %q แล้ว %q, ต้องการ 1@1m0s แล้ว 2@2m0s "+
				"(waiter ที่แพ้ต้องตั้ง timer ใหม่ ไม่ใช่หลับยาว)", first, second)
		}
	})
}

// ── Close ─────────────────────────────────────────────────────────────

// ปิดช่อง statement coverage (Enqueue-after-Close, Close ซ้ำ) และฆ่า mutant
// `ลบ q.mu.Unlock()` ในสาขา closed ของ Dequeue — เดิมไม่มีใครแตะคิวต่อหลังเจอ ErrClosed
// จึง deadlock เงียบ ๆ โดยเทสไม่ร้อง
func TestClosedQueueSemantics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 3, time.Minute)
		mustEnqueue(t, q, &Job{ID: "ready"})
		mustEnqueue(t, q, &Job{ID: "delayed", RunAt: time.Now().Add(time.Hour)})

		q.Close()
		q.Close() // ต้อง idempotent — ไม่ panic เพราะปิด channel ซ้ำ

		if err := q.Enqueue(&Job{ID: "new"}); !errors.Is(err, ErrClosed) {
			t.Errorf("Enqueue หลัง Close = %v, ต้องการ ErrClosed", err)
		}
		// drain: งานที่ ready อยู่แล้วต้องได้ทำต่อจนหมด (ไม่ทิ้งงานที่รับปากไปแล้ว)
		j, err := q.Dequeue(context.Background())
		if err != nil || j.ID != "ready" {
			t.Fatalf("Close ต้อง drain ready ก่อน: %+v, %v", j, err)
		}
		if err := q.Ack(j.ID); err != nil {
			t.Errorf("Ack หลัง Close = %v — ทุก method ต้องยังทำงานได้ ไม่ deadlock", err)
		}
		// งาน delayed ถูกทิ้งตามที่ doc บอก — Dequeue ไม่รอให้ถึง RunAt
		if _, err := q.Dequeue(context.Background()); !errors.Is(err, ErrClosed) {
			t.Errorf("= %v, ต้องการ ErrClosed (งาน delayed ถูกทิ้งตอนปิด)", err)
		}
		if s := q.Stats(); s.Delayed != 1 {
			t.Errorf("Stats หลัง Close = %+v — ต้องยังตอบได้ ไม่ deadlock", s)
		}
	})
}

// ── §4.5 Backoff: ขอบเขตอย่างเดียวไม่พอ ───────────────────────────────

// mutant ที่รอด: `100 * time.Millisecond` → `100 / time.Millisecond` (=0),
// `<<` → `>>`, ทั้งคู่ทำให้ backoff เป็น ~0 เสมอ ซึ่ง "อยู่ในช่วง [0, ceiling]" ยังจริงอยู่
// → retry ถล่มปลายทางที่กำลังฟื้นตัว ทั้งที่เทสเขียว
func TestBackoffGrowsAndSaturates(t *testing.T) {
	const samples = 400
	maxAt := func(attempt int) time.Duration {
		var m time.Duration
		for range samples {
			if d := Backoff(attempt); d > m {
				m = d
			}
		}
		return m
	}

	// ต้องโตแบบ exponential จริง: ค่าสูงสุดที่สุ่มได้ต้องเข้าใกล้เพดานของ attempt นั้น
	for _, c := range []struct{ attempt, wantAtLeast int }{
		{1, 150}, {2, 300}, {3, 600}, {4, 1200},
	} {
		if got := maxAt(c.attempt); got < time.Duration(c.wantAtLeast)*time.Millisecond {
			t.Errorf("Backoff(%d) สูงสุดจาก %d ครั้ง = %v, ต้อง ≥ %dms (backoff ไม่โตตาม attempt)",
				c.attempt, samples, got, c.wantAtLeast)
		}
	}
	// ต้องอิ่มตัวที่เพดาน 30s ไม่โตต่อไปไม่จำกัด
	//
	// เกณฑ์ 27s ไม่ใช่ 29s: ค่าสุ่มสม่ำเสมอใน [0,30s] จะไม่ถึง 29s ทั้ง 400 ครั้ง
	// ด้วยความน่าจะเป็น (29/30)^400 ≈ 1.3e-6 — คูณ -count=20 ใน CI แล้วเจอปีละครั้ง
	// ที่ 27s เหลือ 5e-19 และยังพิสูจน์ "อิ่มตัวใกล้เพดาน" ได้เท่าเดิม
	// (mutant ที่ต้องฆ่าทำให้ backoff เป็น ~0 — เกณฑ์ไหนที่ห่างจาก 0 ก็ฆ่าได้พอกัน)
	if got := maxAt(30); got < 27*time.Second {
		t.Errorf("Backoff(30) สูงสุด = %v, ต้องเข้าใกล้เพดาน 30s", got)
	}
	if got := maxAt(1000); got > 30*time.Second {
		t.Errorf("Backoff(1000) = %v, ต้องไม่เกินเพดาน 30s", got)
	}
	// full jitter: ต้องกระจาย ไม่ใช่ค่าคงที่ (thundering herd กลับมาถ้าไม่สุ่ม)
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[Backoff(10)] = true
	}
	if len(seen) < 25 {
		t.Errorf("Backoff(10) ให้ค่าต่างกันแค่ %d จาก 50 ครั้ง — jitter หายไป", len(seen))
	}
}

// ── ขอบเขตของ config: ค่าที่เล็กที่สุดที่ยังถูกต้อง ต้องผ่าน ──────────────

// mutant ที่รอด: `capacity < 1` → `<= 1`, `visibility <= 0` → `<= 1`
// เทสเดิมยืนยันแค่ "ค่าผิดต้อง panic" ไม่เคยยืนยันว่า "ค่าถูกต้องไม่ panic"
func TestConfigBoundaryAccepted(t *testing.T) {
	for _, c := range []struct {
		name                 string
		capacity, maxAttempt int
		visibility           time.Duration
	}{
		{"ค่าน้อยสุดที่ถูกต้อง", 1, 1, time.Nanosecond},
		{"ปกติ", 100, 3, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic ทั้งที่ config ถูกต้อง: %v", r)
				}
			}()
			q := NewMemQueue(c.capacity, c.maxAttempt, c.visibility)
			if err := q.Enqueue(&Job{ID: "j"}); err != nil {
				t.Errorf("capacity=%d ต้องรับงานได้ 1 ชิ้น: %v", c.capacity, err)
			}
		})
	}
}

// mutant ที่รอด: `d <= 0` → `d <= 1` ทำให้ Extend(1ns) ถูกปฏิเสธ
func TestExtendAcceptsSmallestPositive(t *testing.T) {
	q := NewMemQueue(10, 3, time.Minute)
	mustEnqueue(t, q, &Job{ID: "j"})
	j := mustDequeue(t, q)
	if err := q.Extend(j.ID, time.Nanosecond); err != nil {
		t.Errorf("Extend(1ns) = %v, ต้องผ่าน — ค่าบวกที่เล็กที่สุดยังถูกต้อง", err)
	}
	if err := q.Extend(j.ID, -time.Nanosecond); err == nil {
		t.Error("Extend(-1ns) ต้องถูกปฏิเสธ")
	}
}

// ── §4.1 FIFO ภายใน priority เดียวกัน ─────────────────────────────────

// mutant ที่รอด: `a.Priority > b.Priority` → `>=`, `a.seq < b.seq` → `<=`
// ทั้งคู่ทำให้ตัวเปรียบเทียบไม่เป็น strict weak ordering → ลำดับเพี้ยนแบบไม่แน่นอน
// เทสเดิมมีแค่ 5 งาน heap เล็กเกินกว่าจะเห็น
func TestFIFOStrictUnderManyEqualPriority(t *testing.T) {
	const n = 200
	q := NewMemQueue(n*2, 3, time.Minute)
	for i := range n {
		// สลับ priority ไปมาเพื่อบังคับให้ heap ต้อง sift ของจริง
		mustEnqueue(t, q, &Job{ID: fmt.Sprintf("p%d-%03d", i%3, i), Priority: i % 3})
	}

	var got []string
	for range n {
		got = append(got, mustDequeue(t, q).ID)
	}
	// ต้องออกเป็นบล็อกตาม priority (2,1,0) และภายในบล็อกต้องเรียงตามลำดับที่เข้ามาเป๊ะ
	lastPrio, lastSeq := 3, -1
	for _, id := range got {
		var p, seq int
		fmt.Sscanf(id, "p%d-%03d", &p, &seq)
		switch {
		case p > lastPrio:
			t.Fatalf("%s: priority %d มาหลัง %d — เรียงผิด: %v", id, p, lastPrio, got)
		case p < lastPrio:
			lastPrio, lastSeq = p, -1
		}
		if seq <= lastSeq {
			t.Fatalf("%s: seq %d มาหลัง %d — FIFO ภายใน priority เดียวกันพัง: %v", id, seq, lastSeq, got)
		}
		lastSeq = seq
	}
}

// ── §4.6 RunPool: ต้อง Ack ตอนสำเร็จ และ Nack ตอนล้ม ────────────────────

// mutant ที่รอด: ลบ q.Ack / ลบ q.Nack / ลบ wg.Wait ใน RunPool
// เทสเดิมรอด เพราะ lease หมดอายุกลบร่องรอยให้: งานที่ไม่ถูก ack ก็กลับมาเองอยู่ดี
// เทสนี้ตั้ง visibility ยาวมากจน "กลับมาเอง" เป็นไปไม่ได้ → เหลือแค่ Ack/Nack ที่ทำให้ผ่าน
func TestRunPoolAcksAndNacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 2, time.Hour) // lease ยาว: ถ้าไม่ Ack/Nack เอง จะไม่มีอะไรเกิดขึ้นเลย
		mustEnqueue(t, q, &Job{ID: "ok"})
		mustEnqueue(t, q, &Job{ID: "fail"})

		var mu sync.Mutex
		attempts := map[string]int{}

		ctx, cancel := context.WithCancel(context.Background())
		pool := make(chan struct{})
		go func() {
			defer close(pool)
			RunPool(ctx, q, 2, time.Second, func(ctx context.Context, j *Job) error {
				mu.Lock()
				attempts[j.ID]++
				n := attempts[j.ID]
				mu.Unlock()
				if j.ID == "fail" {
					return fmt.Errorf("ล้มรอบ %d", n)
				}
				return nil
			})
		}()

		// fail ต้องถูก Nack แล้ววน retry จนครบ maxAttempt=2 → DLQ
		// ถ้า Nack หายไป จะไม่มีวันถึงตรงนี้ (lease 1 ชั่วโมง)
		for q.Stats().Dead == 0 {
			time.Sleep(time.Second)
		}
		cancel()
		<-pool

		mu.Lock()
		defer mu.Unlock()
		if attempts["ok"] != 1 {
			t.Errorf("งานสำเร็จถูกเรียก %d ครั้ง, ต้องการ 1 (ไม่ Ack = ทำซ้ำ)", attempts["ok"])
		}
		if attempts["fail"] != 2 {
			t.Errorf("งานที่ล้มถูกเรียก %d ครั้ง, ต้องการ 2 (= maxAttempt)", attempts["fail"])
		}
		// Ack จริงต้องทำให้ ok หายไปจากระบบ ไม่ใช่ค้าง inflight รอ lease หมด
		if s := q.Stats(); s.Inflight != 0 || s.Ready != 0 || s.Delayed != 0 || s.Dead != 1 {
			t.Errorf("Stats = %+v, ต้องการเหลือแค่ Dead=1 (ok ถูก Ack, fail ลง DLQ)", s)
		}
		if d := q.Dead(); len(d) != 1 || d[0].ID != "fail" || !strings.Contains(d[0].LastErr, "ล้มรอบ 2") {
			t.Errorf("DLQ = %+v, ต้องมี fail พร้อมสาเหตุครั้งล่าสุด", d)
		}
	})
}

// RunPool ต้องไม่คืนค่าก่อน worker ทุกตัวออกจริง (mutant: ลบ wg.Wait)
// ไม่งั้น graceful shutdown = คำโกหก: โปรเซสจบขณะงานยังทำอยู่ครึ่งทาง
func TestRunPoolWaitsForWorkers(t *testing.T) {
	q := NewMemQueue(100, 3, time.Minute)
	for i := range 20 {
		mustEnqueue(t, q, &Job{ID: fmt.Sprintf("j%02d", i)})
	}
	ctx, cancel := context.WithCancel(context.Background())

	var inHandler, maxSeen int64
	var mu sync.Mutex
	started := make(chan struct{})
	var once sync.Once

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunPool(ctx, q, 8, time.Second, func(ctx context.Context, j *Job) error {
			mu.Lock()
			inHandler++
			mu.Unlock()
			once.Do(func() { close(started) })
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			inHandler--
			mu.Unlock()
			return nil
		})
	}()

	<-started
	cancel()
	<-done // RunPool คืนค่าแล้ว = worker ทุกตัวต้องออกหมดแล้ว

	mu.Lock()
	maxSeen = inHandler
	mu.Unlock()
	if maxSeen != 0 {
		t.Errorf("RunPool คืนค่าขณะยังมี handler ทำงานอยู่ %d ตัว — wg.Wait หายไป", maxSeen)
	}
}

// ── หน่วยความจำ: heap ต้องไม่ถือ pointer ของงานที่ pop ออกไปแล้ว ──────────

// mutant ที่รอด: ลบ `h.jobs[n-1] = nil` ใน Pop
// → backing array ถือ *Job ที่ไม่มีใครใช้แล้วไว้ตลอดกาล; คิวที่ peak 100k
// จะกิน RAM ระดับ peak ไปจนโปรเซสตาย ทั้งที่ Stats บอกว่าคิวว่าง
func TestPoppedJobNotRetained(t *testing.T) {
	q := NewMemQueue(10, 3, time.Minute)
	ref := func() weak.Pointer[Job] {
		j := &Job{ID: "ใหญ่", Payload: make([]byte, 1<<16)}
		mustEnqueue(t, q, j)
		k := mustDequeue(t, q) // pop ออกจาก ready heap
		if err := q.Ack(k.ID); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		return weak.Make(j)
	}()

	for range 3 { // weak pointer ถูกล้างตอน GC — บางครั้งต้องมากกว่าหนึ่งรอบ
		runtime.GC()
	}
	alive := ref.Value() != nil
	// ★ ต้องอ้างถึง q หลัง GC ไม่งั้น GC เก็บทั้งคิวทิ้ง แล้วเทสจะผ่านเสมอ
	// ไม่ว่า heap จะถือ pointer ค้างหรือไม่ (เทสที่ผ่านเพราะเหตุผลผิด = เทสที่ไม่มีอยู่)
	runtime.KeepAlive(q)
	if alive {
		t.Error("งานที่ ack ไปแล้วยังถูก heap ถือไว้ — backing array ไม่ถูกล้าง (memory leak)")
	}
}

// ── ตัวช่วย: เทสที่ไม่เช็ค error คือเทสที่ผ่านตอนโค้ดพัง ──────────────────

func mustEnqueue(t *testing.T, q *MemQueue, j *Job) {
	t.Helper()
	if err := q.Enqueue(j); err != nil {
		t.Fatalf("Enqueue(%s) = %v", j.ID, err)
	}
}

func mustDequeue(t *testing.T, q *MemQueue) *Job {
	t.Helper()
	j, err := q.Dequeue(t.Context())
	if err != nil {
		t.Fatalf("Dequeue = %v", err)
	}
	return j
}

// ── รอบที่สอง: รูที่ mutation operator ชุดนี้เอื้อมไม่ถึง ────────────────────
//
// mutation testing จำกัดอยู่ที่ operator ที่เครื่องมือรู้จัก (สลับตัวเปรียบเทียบ,
// ลบ statement, แก้ค่าคงที่) — บั๊กที่ต้อง "เขียนโค้ดเพิ่ม" หรือ "สลับตัวแปร"
// ถึงจะเกิด จะไม่มีวันโผล่ในรายงาน ห้าใช้ score 100% แทนการอ่านโค้ด
//
// ทั้งหมดในบล็อกนี้มาจากการรีวิวโดยตรง และสองข้อแรกเป็น **บั๊กจริงในโค้ดโปรดักชัน**
// ไม่ใช่แค่รูของเทส

// บั๊กจริง #1: Extend ที่ **ย่น** lease ไม่ปลุก waiter → งานถูกส่งซ้ำตอน lease เดิมหมด
// ไม่ใช่ตอนที่ขอไว้ (worker ที่รู้ตัวว่าจะเสร็จเร็วกว่าที่ขอ คืนงานเร็วไม่ได้)
// เทสเดิมมีแต่การ **ต่อ** lease ให้ยาวขึ้น จึงไม่เคยเดินเส้นทางนี้
func TestExtendShorteningWakesWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 5, time.Hour) // lease ยาว 1 ชม.: ถ้าไม่ถูกปลุก จะรอ 1 ชม.
		mustEnqueue(t, q, &Job{ID: "j"})
		mustDequeue(t, q)

		got := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			if _, err := q.Dequeue(context.Background()); err == nil {
				got <- time.Since(start)
			}
		}()
		synctest.Wait() // waiter ตั้ง timer ไว้ที่ leaseUntil เดิม (+1 ชม.) แล้ว

		if err := q.Extend("j", time.Second); err != nil { // ★ ย่นเหลือ 1 วิ
			t.Fatalf("Extend: %v", err)
		}
		select {
		case elapsed := <-got:
			if elapsed != time.Second {
				t.Errorf("ส่งซ้ำหลัง %v, ต้องการ 1s (lease ที่ย่นแล้ว)", elapsed)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("waiter ยังนอนรอ lease เดิม — Extend ที่ย่น lease ไม่ปลุกใคร")
		}
	})
}

// บั๊กจริง #2: Dead() ไม่ promote ก่อนตอบ (ละเมิด I2c ที่ §2.4 ประกาศไว้เอง)
// งานลง DLQ ได้จากใน promoteLocked ด้วย → Dead() กับ Stats().Dead ตอบไม่ตรงกัน
// ในวินาทีเดียวกัน ขึ้นกับว่าใครถูกเรียกก่อน = พฤติกรรมขึ้นกับ traffic ที่ไม่เกี่ยวข้อง
func TestDeadPromotesBeforeReporting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemQueue(10, 1, time.Second) // maxAttempt=1 → lease หมดครั้งเดียวลง DLQ
		mustEnqueue(t, q, &Job{ID: "j"})
		mustDequeue(t, q)
		time.Sleep(2 * time.Second)

		// ★ เรียก Dead() **ก่อน** Stats() — คำตอบต้องไม่ขึ้นกับลำดับการเรียก
		dead := q.Dead()
		if len(dead) != 1 || dead[0].ID != "j" {
			t.Fatalf("Dead() = %+v, ต้องมี j (lease หมดครบโควตาแล้ว)", dead)
		}
		if s := q.Stats(); s.Dead != len(dead) {
			t.Errorf("Stats().Dead = %d แต่ len(Dead()) = %d ในวินาทีเดียวกัน", s.Dead, len(dead))
		}
	})
}

// handler ต้องถูกตัดด้วย timeout ต่อหนึ่งงาน และต้องได้ ctx ที่ **มี deadline** จริง
// ถ้า safeCall ส่ง ctx ตัวนอกให้แทน cctx: งานที่ค้างจะกินสล็อต worker ตลอดกาล
// (mutation operator ชุดนี้เอื้อมไม่ถึง เพราะ `cctx, cancel := ...` เป็น := ไม่ใช่ =)
func TestHandlerDeadlineEnforced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 2 * time.Second
		q := NewMemQueue(10, 1, time.Hour) // lease ยาว: ถ้า handler ไม่ถูกตัด ไม่มีอะไรกู้ให้
		mustEnqueue(t, q, &Job{ID: "ไม่รู้จบ"})

		start := time.Now()
		var elapsed time.Duration
		ctx, cancel := context.WithCancel(context.Background())
		pool := make(chan struct{})
		go func() {
			defer close(pool)
			RunPool(ctx, q, 1, timeout, func(hctx context.Context, j *Job) error {
				switch dl, ok := hctx.Deadline(); {
				case !ok:
					t.Error("handler ไม่ได้รับ deadline — ได้ ctx ตัวนอกมาแทน cctx")
					return errors.New("ไม่มี deadline")
				case dl.Sub(start) != timeout:
					t.Errorf("deadline อยู่ที่ +%v, ต้องการ +%v", dl.Sub(start), timeout)
				}
				<-hctx.Done() // งานที่ไม่รู้จบ — ต้องถูกตัดด้วย timeout เท่านั้น
				elapsed = time.Since(start)
				return hctx.Err()
			})
		}()

		for q.Stats().Dead == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		cancel()
		<-pool

		if elapsed != timeout {
			t.Errorf("handler ถูกตัดที่ %v, ต้องการ %v พอดี", elapsed, timeout)
		}
		if d := q.Dead(); len(d) != 1 || !strings.Contains(d[0].LastErr, "deadline exceeded") {
			t.Errorf("DLQ = %+v, ต้องบันทึกสาเหตุว่า deadline exceeded", d)
		}
	})
}

// promoteLocked ต้องระบาย **ทุกตัวที่ถึงเวลา** ในครั้งเดียว ไม่ใช่ทีละตัว
// ถ้าระบายทีละตัว: คิวที่มีงาน delayed 10k ตัวถึงเวลาพร้อมกัน ต้องรอ 10k รอบ
// ของ Dequeue กว่าจะระบายหมด — ดูเหมือนคิวค้างทั้งที่งานพร้อมทำหมดแล้ว
func TestPromoteDrainsEverythingDue(t *testing.T) {
	const n = 5
	t.Run("delayed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 5, time.Hour)
			due := time.Now().Add(time.Minute)
			for i := range n {
				mustEnqueue(t, q, &Job{ID: fmt.Sprint(i), RunAt: due})
			}
			time.Sleep(2 * time.Minute)
			if s := q.Stats(); s.Ready != n || s.Delayed != 0 { // Stats เรียก promote ครั้งเดียว
				t.Errorf("%+v — promote ครั้งเดียวต้องระบาย delayed ที่ถึงเวลาให้หมด (%d ตัว)", s, n)
			}
		})
	})
	t.Run("lease หมด", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const vt = time.Minute
			q := NewMemQueue(10, 5, vt)
			for i := range n {
				mustEnqueue(t, q, &Job{ID: fmt.Sprint(i)})
			}
			for range n {
				mustDequeue(t, q)
			}
			time.Sleep(2 * vt)
			if s := q.Stats(); s.Ready != n || s.Inflight != 0 {
				t.Errorf("%+v — promote ครั้งเดียวต้องกู้ lease ที่หมดให้หมด (%d ตัว)", s, n)
			}
		})
	})
}

// §4.1: tie-break ภายใน priority เดียวกันต้องเป็น **seq** (ลำดับที่เข้าคิว)
// ไม่ใช่ `enqueued` (อายุ) และไม่ใช่ `Attempt` — สองตัวหลังดูสมเหตุสมผลพอ ๆ กัน
// แต่ให้ผลต่างกันในสองสถานการณ์ที่เกิดจริง และไม่มีเทสไหนแยกมันออกจากกัน
func TestFIFOTieBreakIsSeqNotAgeOrAttempt(t *testing.T) {
	// (ก) replay DLQ ต้องต่อ**ท้าย**คิว ไม่ใช่แซงหน้า
	// I4 บอกว่า enqueued ไม่ถูกรีเซ็ต → งานที่ replay มีอายุเก่าสุดเสมอ
	// ถ้า tie-break ใช้อายุ: ระบาย DLQ 10k ตัวจะแซงหน้า traffic สดทั้งหมด
	t.Run("replay ต่อท้าย", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			q := NewMemQueue(10, 1, time.Minute)
			mustEnqueue(t, q, &Job{ID: "เก่า"})
			j := mustDequeue(t, q)
			if err := q.Nack(j.ID, 0, errors.New("boom")); err != nil { // → DLQ
				t.Fatalf("Nack: %v", err)
			}
			time.Sleep(time.Hour) // งานสด "ใหม่กว่า" งานที่อยู่ใน DLQ มาก

			mustEnqueue(t, q, &Job{ID: "สด1"})
			mustEnqueue(t, q, &Job{ID: "สด2"})
			mustEnqueue(t, q, q.Dead()[0]) // replay

			var got []string
			for range 3 {
				got = append(got, mustDequeue(t, q).ID)
			}
			if want := "[สด1 สด2 เก่า]"; fmt.Sprint(got) != want {
				t.Errorf("ลำดับ = %v, ต้องการ %s — งาน replay ต้องต่อท้าย ไม่ใช่แซงตามอายุ", got, want)
			}
		})
	})

	// (ข) งานที่ retry มาหลายรอบต้องไม่แซงงานที่เข้าคิวก่อน
	// ถ้า tie-break ใช้ Attempt: งานที่ล้มบ่อยจะได้คิวหน้าเรื่อย ๆ = starvation กลับด้าน
	t.Run("retry ไม่แซง", func(t *testing.T) {
		q := NewMemQueue(10, 9, time.Minute)
		mustEnqueue(t, q, &Job{ID: "x"}) // seq 1
		mustEnqueue(t, q, &Job{ID: "y"}) // seq 2
		x, y := mustDequeue(t, q), mustDequeue(t, q)
		if x.ID != "x" || y.ID != "y" {
			t.Fatalf("dequeue = %s, %s", x.ID, y.ID)
		}
		// y ล้มสองรอบ (Attempt=2) ส่วน x ล้มรอบเดียว (Attempt=1)
		for range 2 {
			if err := q.Nack("y", 0, errors.New("boom")); err != nil {
				t.Fatalf("Nack y: %v", err)
			}
			mustDequeue(t, q) // เอา y กลับออกมาให้ Attempt เพิ่ม (x ยัง inflight อยู่)
		}
		if err := q.Nack("x", 0, errors.New("boom")); err != nil {
			t.Fatalf("Nack x: %v", err)
		}
		if err := q.Nack("y", 0, errors.New("boom")); err != nil {
			t.Fatalf("Nack y: %v", err)
		}
		if j := mustDequeue(t, q); j.ID != "x" {
			t.Errorf("ตัวถัดไป = %s, ต้องเป็น x (seq น้อยกว่า) — Attempt ห้ามมีผลต่อลำดับ", j.ID)
		}
	})
}

// สัญญาของ jobHeap.Pop โดยตรง: งานที่ออกจาก heap ต้องได้ index = -1
// เทสผ่าน MemQueue มองไม่เห็นข้อนี้ เพราะ retryLocked ตั้ง -1 ซ้ำให้อีกที
// (สอง statement ที่ครอบกันเอง — ลบตัวใดตัวหนึ่งไม่มีผล ต้องเทสที่ตัว heap ตรง ๆ)
func TestHeapPopClearsIndex(t *testing.T) {
	h := &jobHeap{less: func(a, b *Job) bool { return a.seq < b.seq }}
	a, b := &Job{ID: "a", seq: 1}, &Job{ID: "b", seq: 2}
	heap.Push(h, a)
	heap.Push(h, b)

	if got := heap.Pop(h).(*Job); got != a || got.index != -1 {
		t.Errorf("Pop คืน %q index=%d, ต้องการ a index=-1 (sentinel กัน Remove ลบผิดตัว)", got.ID, got.index)
	}
	if b.index != 0 {
		t.Errorf("ตัวที่เหลือ index = %d, ต้องการ 0", b.index)
	}
	if got := heap.Remove(h, b.index).(*Job); got != b || got.index != -1 {
		t.Errorf("Remove คืน %q index=%d, ต้องการ b index=-1", got.ID, got.index)
	}
	if h.Len() != 0 {
		t.Errorf("heap เหลือ %d ตัว", h.Len())
	}
}
