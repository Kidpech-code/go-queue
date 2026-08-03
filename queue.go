// Package queue — คิวงานในโปรเซส: priority + delay + lease/ack + retry/backoff + DLQ + backpressure
//
// ออกแบบมาให้ตรงกับ semantics ของคิวจริง (SQS/River) เพื่อให้ย้ายไปใช้ของ durable
// ได้โดยไม่ต้องแก้ฝั่ง handler: at-least-once + visibility timeout + DLQ
//
// อ่านคำอธิบายเต็มที่ README.md
package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// ─────────────────────────── 1. โดเมน ───────────────────────────

// Job คือหน่วยงานหนึ่งชิ้น. ห้ามแก้ฟิลด์หลังส่งเข้า Enqueue แล้ว — คิวเป็นเจ้าของ
//
// Dequeue คืน **สำเนา** เสมอ ผู้เรียกจึงอ่านทุกฟิลด์ได้อย่างปลอดภัยแม้ lease หมดไปแล้ว
// (คิวยังเขียนตัวจริงต่อ เช่น Attempt++ ตอนส่งซ้ำ — ถ้าคืน pointer ตัวจริงจะเป็น data race)
type Job struct {
	ID       string    // ต้องไม่ซ้ำ; ใช้เป็น idempotency key ฝั่ง handler ด้วย
	Payload  []byte    // ควรเล็ก (< 2KB — เกณฑ์ TOAST ของ PG); ของใหญ่เก็บ S3 ส่งแค่ key
	Priority int       // มาก = ได้ก่อน
	RunAt    time.Time // ห้ามทำก่อนเวลานี้; zero = ทำได้ทันที
	Attempt  int       // นับตอน Dequeue → worker ที่ตายก็ยังถูกนับ
	LastErr  string    // สาเหตุที่ล้มครั้งล่าสุด — คิวเขียนให้ใต้ล็อกใน Nack เท่านั้น

	enqueued   time.Time // เวลาเข้าคิวครั้งแรก (ไม่รีเซ็ตตอน retry) → ใช้วัด lag
	seq        uint64    // tie-break ให้ FIFO ภายใน priority เดียวกัน
	leaseUntil time.Time
	index      int // ตำแหน่งใน heap ปัจจุบัน — จำเป็นสำหรับ heap.Remove
}

var (
	ErrFull        = errors.New("queue: full")              // backpressure
	ErrClosed      = errors.New("queue: closed")            //
	ErrNotInflight = errors.New("queue: job not in flight") // lease หมดไปแล้ว หรือ ack ซ้ำ
	ErrDuplicateID = errors.New("queue: duplicate job ID")  // ID ซ้ำกับงานที่ยังไม่จบ
	ErrEmptyID     = errors.New("queue: empty job ID")      // ID เป็น idempotency key — ว่างไม่ได้
)

// ─────────────── 2. heap ตัวเดียวใช้ซ้ำ 3 มิติการเรียง ───────────────

type jobHeap struct {
	jobs []*Job
	less func(a, b *Job) bool
}

func (h jobHeap) Len() int           { return len(h.jobs) }
func (h jobHeap) Less(i, j int) bool { return h.less(h.jobs[i], h.jobs[j]) }

func (h jobHeap) Swap(i, j int) {
	h.jobs[i], h.jobs[j] = h.jobs[j], h.jobs[i]
	h.jobs[i].index, h.jobs[j].index = i, j // ต้องอัปเดต index ทุกครั้ง ไม่งั้น Remove ลบผิดตัว
}

func (h *jobHeap) Push(x any) {
	j := x.(*Job)
	j.index = len(h.jobs)
	h.jobs = append(h.jobs, j)
}

// Pop ต้องคืน element ท้าย slice — container/heap สลับ root ไปท้ายให้ก่อนเรียก
func (h *jobHeap) Pop() any {
	n := len(h.jobs)
	j := h.jobs[n-1]
	h.jobs[n-1] = nil // กัน backing array ถือ pointer ค้าง
	h.jobs = h.jobs[:n-1]
	j.index = -1
	return j
}

func (h *jobHeap) peek() *Job {
	if len(h.jobs) == 0 {
		return nil
	}
	return h.jobs[0]
}

// ─────────────────────────── 3. คิว ───────────────────────────

// MemQueue ปลอดภัยต่อการเรียกพร้อมกันจากหลาย goroutine
//
// Invariant: งานหนึ่งตัวอยู่ได้ที่เดียวเท่านั้น — ready ⊎ delayed ⊎ leases ⊎ dead
// (field index จึงชี้ตำแหน่งใน heap ปัจจุบันได้ตัวเดียว)
type MemQueue struct {
	mu          sync.Mutex
	ready       jobHeap             // เรียง (priority desc, seq asc)
	delayed     jobHeap             // เรียง RunAt asc
	leases      jobHeap             // เรียง leaseUntil asc
	inflight    map[string]*Job     // id → job ที่ worker ถืออยู่
	ids         map[string]struct{} // ID ของงานที่ยังไม่จบ (ready ∪ delayed ∪ inflight) — บังคับ I1/I2
	dead        []*Job              // DLQ
	wake        chan struct{}       // condition variable ที่ select() ได้
	seq         uint64
	ackTooLate  int64 // นับ Ack/Nack/Extend ที่มาหลัง lease หมด → SLI
	deadDropped int64 // งานที่ตกจาก DLQ เพราะ DLQ เต็ม → SLI
	closed      bool

	capacity   int
	maxAttempt int
	visibility time.Duration
}

// NewMemQueue สร้างคิว
//
//	capacity   — จำนวนงานสูงสุดในระบบ (ready+delayed+inflight); เกินแล้ว Enqueue คืน ErrFull
//	maxAttempt — ลองกี่ครั้งก่อนตกลง DLQ
//	visibility — lease: ถ้าไม่ Ack/Nack ภายในเวลานี้ งานจะถูกส่งซ้ำ
//	             ต้องมากกว่า p99.9 ของเวลาทำงานจริง ไม่งั้นงานจะวนซ้ำไม่จบ
//
// panic ถ้าค่าไม่สมเหตุสมผล — เป็น programmer error ที่ตรวจได้ตอนบูต ไม่ใช่ตอนตีสาม
// (visibility=0 ทำให้ทุกงานหมด lease ทันทีที่ถูกหยิบ → Ack ไม่มีทางทัน → ทุกงานลง DLQ)
func NewMemQueue(capacity, maxAttempt int, visibility time.Duration) *MemQueue {
	if capacity < 1 || maxAttempt < 1 || visibility <= 0 {
		panic(fmt.Sprintf("queue: NewMemQueue(%d, %d, %v): capacity/maxAttempt ต้อง ≥ 1 และ visibility > 0",
			capacity, maxAttempt, visibility))
	}
	return &MemQueue{
		ready: jobHeap{less: func(a, b *Job) bool {
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			return a.seq < b.seq // heap เป็น partial order — ต้อง tie-break เองถึงจะ FIFO
		}},
		delayed:    jobHeap{less: func(a, b *Job) bool { return a.RunAt.Before(b.RunAt) }},
		leases:     jobHeap{less: func(a, b *Job) bool { return a.leaseUntil.Before(b.leaseUntil) }},
		inflight:   make(map[string]*Job),
		ids:        make(map[string]struct{}),
		wake:       make(chan struct{}),
		capacity:   capacity,
		maxAttempt: maxAttempt,
		visibility: visibility,
	}
}

// Enqueue ไม่บล็อก. คิวเต็มคืน ErrFull ทันที — ให้ผู้เรียกตัดสินใจว่าจะ retry หรือ 429
func (q *MemQueue) Enqueue(j *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	// ID ว่างจะจองสล็อต "" ทั้งคิว → งานที่สองเจอ ErrDuplicateID แทนที่จะรู้ว่าลืมใส่ ID
	if j.ID == "" {
		return ErrEmptyID
	}
	if q.sizeLocked() >= q.capacity {
		return ErrFull // ปฏิเสธดีกว่าโตไม่จำกัด — คิวไม่มีขอบ = OOM + latency พุ่ง
	}
	// ID ซ้ำจะทำให้ inflight[id] ถูกเขียนทับ → I2 พัง → Stats โกหก + งานถูกทำซ้ำ
	// ตรวจที่นี่เพราะเป็นขอบเขตความเชื่อถือ ไม่ใช่ปล่อยให้พังเงียบ ๆ ข้างใน
	if _, dup := q.ids[j.ID]; dup {
		return ErrDuplicateID
	}
	q.ids[j.ID] = struct{}{}
	q.seq++
	j.seq = q.seq
	j.Attempt = 0 // เข้าคิวใหม่ = เริ่มนับใหม่; ไม่งั้น replay จาก Dead() เด้งกลับ DLQ ทันที
	if j.enqueued.IsZero() {
		j.enqueued = time.Now()
	}
	q.pushLocked(j)
	q.broadcastLocked()
	return nil
}

// Dequeue บล็อกจนกว่าจะมีงาน / ctx ถูกยกเลิก / คิวปิด
//
// งานที่คืนมาถือ lease ไว้ visibility — ต้อง Ack หรือ Nack ก่อนหมดอายุ
// ไม่งั้นงานจะถูกส่งให้ worker ตัวอื่นทำซ้ำ (at-least-once)
//
// คืน **สำเนา** ของ Job: ตัวจริงยังอยู่กับคิวและถูกเขียนต่อเมื่อ lease หมด
// ctx ที่ถูกยกเลิกแล้วจะไม่ได้งานแม้คิวจะมีงานรออยู่ — ไม่งั้น shutdown จะกวาด
// ready ทั้งกองมาเผา Attempt ทิ้ง (deploy N ครั้ง = ทุกงานในคิวเสีย N ครั้ง)
func (q *MemQueue) Dequeue(ctx context.Context) (*Job, error) {
	// timer ตัวเดียวใช้ซ้ำทั้งการวน — broadcast ปลุก waiter ทุกตัว ถ้าสร้างใหม่ทุกรอบ
	// จะ alloc เป็นสัดส่วนกับจำนวน worker (วัดได้: 31 allocs/op ที่ 512 worker)
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for { // วนเพราะ broadcast ปลุก waiter ทุกตัว แต่งานมีชิ้นเดียว (spurious wakeup by design)
		if err := ctx.Err(); err != nil {
			return nil, err // เช็กก่อนแตะคิว: ยกเลิกแล้วต้องไม่รับงานเพิ่ม
		}
		q.mu.Lock()
		now := time.Now()
		q.promoteLocked(now)

		if q.ready.Len() > 0 {
			j := heap.Pop(&q.ready).(*Job)
			j.Attempt++ // นับที่นี่ ไม่ใช่ที่ Nack — งานที่ทำ worker ตายจะได้ไม่วนตลอดกาล
			j.leaseUntil = now.Add(q.visibility)
			q.inflight[j.ID] = j
			heap.Push(&q.leases, j)
			cp := *j // สำเนาใต้ล็อก — worker อ่านได้ปลอดภัยแม้คิวจะแจกตัวจริงให้คนอื่นต่อ
			q.mu.Unlock()
			return &cp, nil
		}
		if q.closed {
			q.mu.Unlock()
			return nil, ErrClosed
		}
		wake := q.wake // จับ ref ใต้ล็อกเดียวกับผู้ปลุก → เป็นไปไม่ได้ที่จะพลาดสัญญาณ
		d, has := q.nextEventLocked(now)
		q.mu.Unlock()

		// ponytail: ตื่นเองตามเวลา event ถัดไป แทนที่จะมี goroutine reaper แยก
		// (ไม่มีใคร Dequeue = ไม่มีใครเดือดร้อนว่า lease หมด)
		var timerC <-chan time.Time
		if has {
			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				timer.Reset(d) // Go 1.23+: channel เป็น unbuffered แล้ว ไม่ต้อง drain ก่อน Reset
			}
			timerC = timer.C // nil channel = บล็อกตลอดกาล เมื่อไม่มี event รออยู่
		}
		select {
		case <-wake: // มีงานใหม่ / มีคน nack / มีการ promote
		case <-timerC: // ถึงเวลา delayed หรือ lease หมดอายุ
		case <-ctx.Done():
			return nil, ctx.Err() // timer ถูก Stop ใน defer
		}
		if has {
			timer.Stop()
		}
	}
}

// Ack ยืนยันว่างานสำเร็จ. คืน ErrNotInflight ถ้า lease หมดไปแล้ว
// (แปลว่างานถูกส่งซ้ำให้คนอื่นแล้ว — สัญญาณว่า visibility สั้นเกินไป)
func (q *MemQueue) Ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteLocked(time.Now()) // ต้อง promote ก่อนเสมอ ไม่งั้นตัดสินจาก state ที่ล้าสมัย
	j, ok := q.inflight[id]
	if !ok {
		q.ackTooLate++ // ← SLI: lease หมดก่อน ack เสร็จ = visibility timeout สั้นเกินไป
		return ErrNotInflight
	}
	delete(q.inflight, id)
	delete(q.ids, id) // งานจบแล้ว — ปล่อย ID ให้ใช้ซ้ำได้
	heap.Remove(&q.leases, j.index)
	return nil
}

// Extend ต่ออายุ lease ของงานที่ยังทำอยู่ (เทียบเท่า ChangeMessageVisibility ของ SQS)
//
// worker ที่ทำงานนานกว่า visibility ต้องเรียกตัวนี้เป็นระยะ (ทุก visibility/3)
// ถ้าคืน ErrNotInflight แปลว่าเสีย lease ไปแล้ว — ต้อง **หยุดทำงานทันที**
// เพราะมี worker อื่นรับงานไปทำแล้ว การทำต่อคือการทำซ้ำที่ป้องกันได้
func (q *MemQueue) Extend(id string, d time.Duration) error {
	if d <= 0 {
		// d ≤ 0 = ตั้ง lease ไว้ในอดีต → งานถูกแจกซ้ำเงียบ ๆ ทั้งที่ worker ยังทำอยู่
		return fmt.Errorf("queue: Extend(%q, %v): duration ต้อง > 0", id, d)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteLocked(time.Now())
	j, ok := q.inflight[id]
	if !ok {
		q.ackTooLate++
		return ErrNotInflight
	}
	prev := j.leaseUntil
	j.leaseUntil = time.Now().Add(d)
	heap.Fix(&q.leases, j.index) // ค่าที่ใช้เรียงเปลี่ยน → ต้อง Fix ไม่ใช่ปล่อยไว้
	// ปลุกเฉพาะตอน **ย่น** lease (SQS ก็ยอมให้ย่น): waiter ตั้ง timer ไว้ที่ leaseUntil เดิม
	// ซึ่งตอนนี้ช้าเกินจริง — ไม่ปลุกให้ตั้งใหม่ งานจะถูกส่งซ้ำตอน lease เดิมหมด
	//
	// ต่อให้ยาวขึ้นไม่ต้องปลุก: waiter จะตื่นที่เวลาเดิม เห็นว่ายังไม่ถึงคิว แล้วตั้งใหม่เอง
	// เงื่อนไขนี้ไม่ใช่การรีดประสิทธิภาพเล่น ๆ — heartbeat คือ path ที่ถูกเรียกถี่ที่สุดของ
	// งานยาว (ทุก visibility/3) และ broadcast ปลุก waiter **ทุกตัว**
	//
	// วัดได้ที่ BenchmarkExtendHeartbeat512Waiters: ~2,180 ns/op · 1 alloc ถ้าปลุกทุกครั้ง
	// เทียบกับ ~122 ns/op · 0 alloc เมื่อมีเงื่อนไข (เร็วขึ้น ~18 เท่า)
	// เทสทั้งชุดเขียวทั้งสองแบบ — benchmark คือสิ่งเดียวที่เห็นความต่าง
	if j.leaseUntil.Before(prev) {
		q.broadcastLocked()
	}
	return nil
}

// Nack แจ้งว่างานล้มเหลว → เข้าคิวใหม่หลัง delay หรือตกลง DLQ ถ้าครบ maxAttempt
//
// cause บันทึกลง Job.LastErr เพื่อให้ DLQ วินิจฉัยได้ (nil ได้)
// รับ error ตรงนี้แทนที่จะให้ worker เขียน j.LastErr เอง เพราะ lease อาจหมดไปแล้ว
// และมี worker อื่นถือ *Job ตัวเดียวกันอยู่ → เขียนนอกล็อกคือ data race
func (q *MemQueue) Nack(id string, delay time.Duration, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteLocked(time.Now())
	j, ok := q.inflight[id]
	if !ok {
		q.ackTooLate++
		return ErrNotInflight
	}
	if cause != nil {
		j.LastErr = cause.Error()
	}
	delete(q.inflight, id)
	heap.Remove(&q.leases, j.index)
	q.retryLocked(j, delay)
	q.broadcastLocked()
	return nil
}

// Close หยุดรับงานใหม่และปลุก Dequeue ทุกตัวให้คืน ErrClosed
// งานใน delayed ถูกทิ้ง — ยอมรับได้เพราะคิวนี้อยู่ในหน่วยความจำอยู่แล้ว
func (q *MemQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.broadcastLocked()
}

// ───────────────── 4. ส่วนภายใน (ผู้เรียกถือล็อกอยู่แล้ว) ─────────────────

// sizeLocked = |ready ∪ delayed ∪ inflight| ซึ่งตรงกับ ids พอดีตามนิยาม
func (q *MemQueue) sizeLocked() int { return len(q.ids) }

func (q *MemQueue) pushLocked(j *Job) {
	if j.RunAt.After(time.Now()) {
		heap.Push(&q.delayed, j)
	} else {
		heap.Push(&q.ready, j)
	}
}

// broadcastLocked ปลุก waiter ทุกตัว. ปิด channel = broadcast; สร้างใหม่สำหรับรอบถัดไป
func (q *MemQueue) broadcastLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// promoteLocked ย้าย delayed ที่ถึงเวลา และ lease ที่หมดอายุ กลับเข้า ready
func (q *MemQueue) promoteLocked(now time.Time) {
	moved := false
	for h := q.delayed.peek(); h != nil && !h.RunAt.After(now); h = q.delayed.peek() {
		heap.Push(&q.ready, heap.Pop(&q.delayed).(*Job))
		moved = true
	}
	for h := q.leases.peek(); h != nil && !h.leaseUntil.After(now); h = q.leases.peek() {
		j := heap.Pop(&q.leases).(*Job)
		delete(q.inflight, j.ID)
		q.retryLocked(j, 0) // worker ตาย → ส่งซ้ำทันที (Attempt นับไปแล้วตอน Dequeue)
		moved = true
	}
	if moved {
		q.broadcastLocked()
	}
}

func (q *MemQueue) retryLocked(j *Job, delay time.Duration) {
	if j.Attempt >= q.maxAttempt {
		j.index = -1
		delete(q.ids, j.ID) // ลง DLQ = จบวงจร; ID ว่างให้ enqueue ใหม่ได้หลังแก้บั๊ก
		// ponytail: DLQ มีขอบเท่า capacity แล้วทิ้ง "ตัวใหม่" ไม่ใช่ตัวเก่า — O(1)
		// และ N ตัวแรกคือตัวที่ใช้วินิจฉัย ตัวหลัง ๆ มักสาเหตุเดียวกัน
		// ถ้า DeadDropped > 0 แปลว่าไม่มีใครระบาย DLQ = ปัญหาที่ใหญ่กว่าการเก็บครบ
		if len(q.dead) >= q.capacity {
			q.deadDropped++
			return
		}
		q.dead = append(q.dead, j) // poison pill → DLQ ไม่วนไม่รู้จบ
		return
	}
	j.RunAt = time.Now().Add(delay)
	q.pushLocked(j)
}

// nextEventLocked รวม delayed กับ lease-expiry เป็น timeline เดียว → timer ตัวเดียวพอ
func (q *MemQueue) nextEventLocked(now time.Time) (time.Duration, bool) {
	var t time.Time
	if h := q.delayed.peek(); h != nil {
		t = h.RunAt
	}
	if h := q.leases.peek(); h != nil && (t.IsZero() || h.leaseUntil.Before(t)) {
		t = h.leaseUntil
	}
	if t.IsZero() {
		return 0, false
	}
	return max(t.Sub(now), 0), true
}

// ─────────────────────────── 5. Metrics ───────────────────────────

type Stats struct {
	Ready, Delayed, Inflight, Dead int
	OldestReady                    time.Duration // ← SLI ที่ควร alert ไม่ใช่ depth
	AckTooLate                     int64         // ← สะสม: visibility timeout สั้นเกินไป
	DeadDropped                    int64         // ← สะสม: DLQ ล้น ไม่มีใครระบาย
}

// Stats สแกน ready เป็น O(n) — ตั้งใจให้เรียกจาก metrics scraper (ทุก 10 วิ) ไม่ใช่ hot path
//
// มี side effect โดยตั้งใจ: promote งาน delayed ที่ถึงเวลาและ lease ที่หมดอายุก่อนนับ
// ไม่งั้นตัวเลขที่รายงานจะล้าหลังความจริงตามระยะที่ไม่มีใครเรียก Dequeue
func (q *MemQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promoteLocked(time.Now())
	s := Stats{q.ready.Len(), q.delayed.Len(), len(q.inflight), len(q.dead), 0,
		q.ackTooLate, q.deadDropped}
	now := time.Now()
	for _, j := range q.ready.jobs {
		if age := now.Sub(j.enqueued); age > s.OldestReady {
			s.OldestReady = age
		}
	}
	return s
}

// Dead คืนสำเนาของ DLQ — สำเนาลึกถึงตัว Job เพื่อให้ replay ทำได้ตรง ๆ:
// ส่งตัวที่ได้กลับเข้า Enqueue ได้เลยโดยไม่ทำให้ *Job ตัวเดียวอยู่ทั้งใน dead และในคิว
//
// มี side effect เช่นเดียวกับ Stats โดยตั้งใจ: promote ก่อนอ่านเสมอ (I2c)
// งานลง DLQ ได้จากใน promoteLocked → ถ้าไม่ promote ก่อน Dead() กับ Stats().Dead
// จะตอบไม่ตรงกันในวินาทีเดียวกัน ขึ้นกับว่าใครถูกเรียกก่อน
func (q *MemQueue) Dead() []*Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	// I2c: งานลง DLQ ได้จากใน promoteLocked (lease หมดครั้งสุดท้าย) — ไม่ promote ก่อน
	// แปลว่า Dead() กับ Stats().Dead ตอบไม่ตรงกันได้ในวินาทีเดียวกัน ขึ้นกับว่าใครถูกเรียกก่อน
	q.promoteLocked(time.Now())
	out := make([]*Job, len(q.dead))
	for i, j := range q.dead {
		cp := *j
		out[i] = &cp
	}
	return out
}

// ─────────────────────── 6. Worker pool ───────────────────────

// Handler ต้อง idempotent — at-least-once แปลว่างานจะถูกเรียกซ้ำแน่นอน
// และต้องเคารพ ctx (คืนทันทีเมื่อ ctx.Done) ไม่งั้น graceful shutdown ไม่ทำงาน
type Handler func(ctx context.Context, j *Job) error

// Backoff: exponential + full jitter (AWS) — jitter กัน thundering herd ตอนปลายทางฟื้น
func Backoff(attempt int) time.Duration {
	const base, ceiling = 100 * time.Millisecond, 30 * time.Second
	d := min(base<<min(attempt, 20), ceiling) // min(attempt,20) กัน shift overflow
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// RunPool บล็อกจนกว่า worker ทุกตัวจะออก (ctx ถูกยกเลิก หรือคิวถูกปิด)
// เรียกใน goroutine แล้วรอ ถ้าต้องการ graceful shutdown แบบมี deadline
func RunPool(ctx context.Context, q *MemQueue, workers int, timeout time.Duration, h Handler) {
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1) // Add ก่อน go เสมอ ไม่งั้น Wait อาจผ่านก่อน goroutine เริ่ม
		go func() {
			defer wg.Done()
			for {
				j, err := q.Dequeue(ctx)
				if err != nil {
					return // ctx.Err() หรือ ErrClosed → ออกอย่างสงบ
				}
				if err := safeCall(ctx, timeout, h, j); err != nil {
					q.Nack(j.ID, Backoff(j.Attempt), err) // err → Job.LastErr เพื่อวินิจฉัย DLQ
					continue
				}
				q.Ack(j.ID)
			}
		}()
	}
	wg.Wait()
}

// safeCall: งานหนึ่งตัว panic ต้องไม่ล้มทั้งโปรเซส + บังคับ timeout ต่อหนึ่งงาน
//
// ข้อจำกัด: recover ทำงานเฉพาะใน goroutine ของตัวเอง — ถ้า handler ไป go func() แล้ว
// panic ข้างใน ตัวนี้ช่วยไม่ได้
func safeCall(ctx context.Context, timeout time.Duration, h Handler, j *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel() // ไม่ cancel = ctx leak สะสม
	return h(cctx, j)
}
