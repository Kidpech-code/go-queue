# go-queue — ระบบคิวงานระดับ Production

[![ci](https://github.com/Kidpech-code/go-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Kidpech-code/go-queue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Kidpech-code/go-queue/branch/main/graph/badge.svg)](https://codecov.io/gh/Kidpech-code/go-queue)
[![mutation score](https://img.shields.io/badge/mutation%20score-100%25-brightgreen)](#8-การเทส--coverage-สเปก-และ-เทสจับบั๊กได้จริงไหม)
[![Go Reference](https://pkg.go.dev/badge/github.com/Kidpech-code/go-queue.svg)](https://pkg.go.dev/github.com/Kidpech-code/go-queue)

เอกสาร **แนวคิด → ตรรกะ → โครงสร้าง → อัลกอริทึม** พร้อม implementation ที่รันได้จริง

```bash
make ci     # lint + race + coverage 100% + mutation score 100%
```

| ไฟล์ | เนื้อหา |
|---|---|
| `README.md` | เอกสารนี้ |
| `queue.go` | `MemQueue` (priority + delay + lease/ack + retry + DLQ) และ `RunPool` |
| `queue_test.go` | เทสพื้นฐานทุกเส้นทาง ใช้ `testing/synctest` (นาฬิกาปลอม → เร็วและ deterministic) |
| `spec_test.go` | เทสผูกกับสเปกใน §2.3/§2.4 + หนึ่งเทสต่อหนึ่งคลาสของ mutant ที่เคยรอด |
| `invariant_test.go` | ตัวตรวจ invariant, กฎอนุรักษ์, model-based, fuzz, stress ใต้ `-race` |
| `tools/mutate/` | เครื่องมือ mutation testing (stdlib ล้วน) — และเทสของตัวมันเอง |
| `mutation-allow.txt` | mutant ที่พิสูจน์แล้วว่า equivalent พร้อมเหตุผลทีละตัว |

> **ตัวเลขที่ใช้ตัดสิน** ([§8](#8-การเทส--coverage-สเปก-และ-เทสจับบั๊กได้จริงไหม)):
> statement coverage **100.0%** ทั้ง repo · mutation score **100.0%** (141 killed / 0 survived
> / 16 equivalent) · 115 test case · `-race` 20 รอบติด · fuzz

**สรุป 30 วินาที** — คิวคือกล่องที่แยก *ผู้ผลิตงาน* ออกจาก *ผู้ทำงาน* เพื่อให้
(1) รับ burst ได้โดยไม่ล้ม (2) retry ได้เมื่อล้มเหลว (3) ขยาย worker ตามโหลดได้อิสระ

ราคาที่จ่ายคือ **งานจะถูกทำซ้ำได้เสมอ** — เอกสารทั้งฉบับนี้คือการจัดการผลพวงของข้อนั้น

---

## สารบัญ

- [0. เลือกก่อนเขียนโค้ด](#0-เลือกก่อนเขียนโค้ด)
- [1. แนวคิด — 7 แกนที่ต้องตัดสินใจก่อน](#1-แนวคิด--7-แกนที่ต้องตัดสินใจก่อน)
- [2. ตรรกะ — state machine, invariant, ลำดับเหตุการณ์](#2-ตรรกะ--state-machine-invariant-ลำดับเหตุการณ์)
- [3. โครงสร้าง — โมดูล, ชนิดข้อมูล, หน่วยความจำ](#3-โครงสร้าง--โมดูล-ชนิดข้อมูล-หน่วยความจำ)
- [4. อัลกอริทึม — 6 ตัวพร้อม complexity](#4-อัลกอริทึม--6-ตัวพร้อม-complexity)
- [5. Production: Postgres + SKIP LOCKED](#5-production-postgres--skip-locked)
- [6. Operations: metric, sizing, failure mode](#6-operations-metric-sizing-failure-mode)
- [7. รีวิว: ข้อจำกัดที่รู้ตัว](#7-รีวิว-ข้อจำกัดที่รู้ตัว)
- [8. การเทส — coverage, สเปก, และ "เทสจับบั๊กได้จริงไหม"](#8-การเทส--coverage-สเปก-และ-เทสจับบั๊กได้จริงไหม)
- [9. ต่อเข้าของจริง — main.go เต็มรูปแบบ](#9-ต่อเข้าของจริง--maingo-เต็มรูปแบบ)
- [ภาคผนวก: คำศัพท์](#ภาคผนวก-คำศัพท์)

---

## 0. เลือกก่อนเขียนโค้ด

คิวส่วนใหญ่ที่ทีมเขียนเองไม่ควรถูกเขียน. ไล่จากบนลงล่าง **หยุดที่ขั้นแรกที่พอ**:

| ต้องการ | ใช้ | สัดส่วนงานจริง |
|---|---|---|
| กระจายงานใน process เดียว งานหายได้ | `chan T` (cap > 0) + worker pool | **~60% จบตรงนี้** |
| + priority / delay / retry / DLQ ในหน่วยความจำ | repo นี้ (~350 บรรทัด) | |
| งานห้ามหายเมื่อ process ตาย และมี Postgres อยู่แล้ว | **Postgres `FOR UPDATE SKIP LOCKED`** หรือ `riverqueue/river` | **~30% จบตรงนี้** |
| throughput สูงมาก มี Redis อยู่แล้ว | `hibiken/asynq` (Redis Streams) | |
| หลาย service, fan-out, replay ย้อนหลัง, event log | NATS JetStream / Kafka (`twmb/franz-go`) | |

> **จุดตัดจริง:** มี Postgres อยู่แล้ว + throughput < ~2,000 job/s → **ใช้ Postgres**
> เพราะได้ **transactional outbox** ฟรี ([§5.3](#53-เหตุผลจริงที่เลือก-postgres--transactional-outbox))
> ซึ่ง Redis/Kafka ให้ไม่ได้เลย. **อย่าลาก Kafka เข้ามาเพื่อ 50 job/s**

### 0.1 ต้นทุนที่มองไม่เห็นของแต่ละขั้น

การเลือกขั้นที่สูงกว่าไม่ได้จ่ายแค่ค่า infra — จ่ายค่าเหล่านี้ด้วย:

| ขั้น | ต้นทุนแฝง |
|---|---|
| `chan` | ไม่มี. อยู่ใน binary เดียว ดีบักด้วย `dlv` ปกติ |
| repo นี้ | ต้องเข้าใจ heap + lease เอง; งานหายเมื่อ deploy |
| Postgres | connection pool ต้องโตตาม worker; ต้องคุม autovacuum ([§5.4](#54-สิ่งที่ต้องทำ-ไม่งั้นเจ็บ)) |
| Redis | เพิ่ม SPOF; persistence ของ Redis อ่อนกว่า Postgres (AOF fsync ทุกวินาที = เสียได้ 1 วิ) |
| Kafka | ต้องมีคนดูแล cluster; ordering/rebalance/consumer-group เป็นศาสตร์ของมันเอง; local dev ช้าลง |

**กฎ:** เพิ่มขั้นเมื่อมี *อาการ* ที่วัดได้ ไม่ใช่เมื่อมี *ความรู้สึก* ว่าจะโต

ที่เหลือของเอกสารนี้อธิบายว่า **ข้างในมันทำงานอย่างไร** — เพื่อให้เลือก จูน และดีบักของจริงได้ถูก

---

## 1. แนวคิด — 7 แกนที่ต้องตัดสินใจก่อน

ทุกดีไซน์คิวคือการเลือกตำแหน่งบนแกนเหล่านี้. **เลือกผิดแล้วแก้ทีหลังแพงมาก**
เพราะ semantics รั่วเข้าไปในโค้ด business ทุกจุด

### 1.1 Queue vs Stream/Log — ต่างกันที่ "ข้อความหายไปไหนหลังอ่าน"

คนมักเรียกรวมว่า "คิว" แต่เป็นคนละสัตว์:

| | **Queue** (SQS, RabbitMQ, repo นี้) | **Log/Stream** (Kafka, Redis Streams) |
|---|---|---|
| หลังอ่านสำเร็จ | ข้อความ **ถูกลบ** | ข้อความ **ยังอยู่**; consumer เก็บแค่ offset |
| ผู้บริโภคหลายราย | แย่งกันทำ (competing consumers) | แต่ละ group อ่านครบทุกข้อความ (fan-out) |
| Replay ย้อนหลัง | ทำไม่ได้ | ทำได้ (ย้อน offset) |
| หน่วยของ concurrency | ข้อความ 1 ชิ้น | partition 1 อัน |
| Retry เฉพาะตัว | ทำได้ตรงไปตรงมา | ทำไม่ได้ตรง ๆ — ต้องมี retry topic (แล้วลำดับพัง) |
| เหมาะกับ | **งาน** (task) — ส่งอีเมล, resize รูป | **เหตุการณ์** (event) — order.created ที่หลายทีมสนใจ |

> **สับสนตรงนี้บ่อยมาก:** ถ้าต้องการ "retry งานที่ล้มเฉพาะตัว" แล้วเลือก Kafka
> คุณจะต้องสร้าง retry topic แยกตาม delay (5s, 1m, 10m) แบบที่ Uber ทำ
> ซึ่ง **ทำลาย ordering** ที่เป็นเหตุผลที่เลือก Kafka ตั้งแต่แรก

repo นี้เป็น **Queue** ล้วน

### 1.2 Delivery semantics — เรื่องของ "Ack เมื่อไหร่" ล้วน ๆ

| แบบ | Ack เมื่อไหร่ | ผล | ใช้เมื่อ |
|---|---|---|---|
| at-most-once | **ก่อน** ประมวลผล | เร็ว แต่งานหายได้ | metric, log, งานที่หายแล้วไม่เป็นไร |
| **at-least-once** | **หลัง** ประมวลผลสำเร็จ | งานไม่หาย แต่ซ้ำได้ | **default ที่ถูกเกือบทุกกรณี** |
| exactly-once | — | **ไม่มีอยู่จริง** ข้ามขอบเขตระบบ | — |

#### ทำไม exactly-once ถึงเป็นไปไม่ได้ (โครงพิสูจน์)

worker ต้องทำสองอย่าง: **(A) ทำงาน** (ตัดบัตร) และ **(B) บันทึกว่าทำแล้ว** (ack)

- ถ้า A ก่อน B: เครื่องดับระหว่างกลาง → งานทำแล้วแต่ไม่มีใครรู้ → **ทำซ้ำ**
- ถ้า B ก่อน A: เครื่องดับระหว่างกลาง → ack แล้วแต่งานไม่ได้ทำ → **งานหาย**

จะไม่ซ้ำและไม่หายพร้อมกันได้ ต้องให้ A กับ B เป็น **atomic operation เดียว**
ซึ่งเป็นไปได้ก็ต่อเมื่อทั้งคู่อยู่ใน transactional resource เดียวกัน

- Kafka EOS ทำได้เพราะ A และ B อยู่ใน Kafka ทั้งคู่ (read→process→write กลับ Kafka)
- Postgres queue + Postgres side effect ก็ทำได้ ด้วยเหตุผลเดียวกัน ([§6.3](#63-idempotency-ไม่ใช่-nice-to-have))
- **ส่งอีเมล / เรียก Stripe** อยู่นอก transaction เสมอ → การันตีนั้นหายทันที

> **ผลตามมาที่หลีกเลี่ยงไม่ได้: handler ต้อง idempotent เสมอ ไม่มีข้อยกเว้น**
> ทุกอย่างที่เหลือคือรายละเอียดของการทำให้ข้อนี้ทำได้จริง

### 1.3 Push vs Pull — และ long-poll ที่เป็นทางสายกลาง

| | Push (broker ยัดให้) | Pull (consumer ขอเอง) |
|---|---|---|
| Flow control | ต้องสร้างเอง (credit/window) | ได้ฟรี — ไม่ขอก็ไม่ได้ |
| Latency | ต่ำสุด | สูงกว่านิดหน่อย |
| Broker ซับซ้อน | มาก (ต้องรู้สภาพ consumer) | น้อย (stateless กว่า) |
| Consumer ตาย | broker ต้องตรวจจับ | ไม่ต้องทำอะไร — แค่ไม่มีใครมาขอ |

**Pull ชนะเกือบทุกกรณี.** ปัญหาเดียวคือ polling เปลืองถ้าคิวว่าง — แก้ด้วย **long-poll**:
consumer บล็อกรออยู่ที่ broker จนกว่าจะมีงานหรือหมดเวลา

- SQS: `ReceiveMessage(WaitTimeSeconds: 20)`
- Postgres: `LISTEN` + `WaitForNotification`
- repo นี้: `Dequeue` **บล็อกอยู่แล้ว** — long-poll โดยธรรมชาติ ([§4.3](#43-condition-variable-ที่-select-ได้--อัลกอริทึมสำคัญที่สุดในไฟล์))

### 1.4 Ordering — และเหตุผลที่ FIFO กับ retry ขัดกันโดยธรรมชาติ

FIFO ทั้งคิวแบบเข้มงวด **บังคับให้ concurrency = 1** — ตัด throughput ทิ้งทั้งหมด

สิ่งที่ต้องการจริงคือ **per-key ordering**: งานของ `user_42` เรียงกัน แต่ต่าง user ขนานกันได้

```
คิวเดียว FIFO เข้มงวด           แบ่งตาม key (partition)
┌──────────────────────┐        ┌─ user_42 ─┐  ┌─ user_99 ─┐  ┌─ user_7 ─┐
│ A → B → C → D → E    │        │ A → C     │  │ B → E     │  │ D        │
└──────────────────────┘        └───────────┘  └───────────┘  └──────────┘
  worker = 1 ตัวเท่านั้น          worker = 3 ตัวขนานกัน
                                  ลำดับภายในแต่ละ user ยังถูก
```

**ความขัดแย้งที่หลีกเลี่ยงไม่ได้:** ถ้างาน B ล้มและต้อง retry ในคิวที่ต้อง FIFO
มีทางเลือกแค่สองทาง และทั้งคู่แย่:

1. **บล็อกทั้งคิว** จนกว่า B จะสำเร็จ → งาน C, D, E ค้างตามไปด้วย
   (SQS FIFO ทำแบบนี้: message group ค้างทั้งกลุ่ม)
2. **ข้าม B ไปทำ C** แล้วส่ง B ไป retry topic → **ลำดับพัง**
   (Kafka + retry topic ทำแบบนี้)

> ถ้าไม่จำเป็น **อย่าใส่ ordering** — repo นี้ให้ FIFO ภายใน priority ชั้นเดียวกันเท่านั้น
> (ทำได้เพราะ tie-break ด้วย `seq` — [§4.1](#41-priority--fifo--binary-heap-ที่มี-tie-break))
> และไม่รับประกันลำดับเมื่อ retry เพราะงานที่ retry จะได้ `seq` เดิมแต่ `RunAt` ใหม่

### 1.5 Bounded เสมอ — และ 4 นโยบายเมื่อเต็ม

คิวไม่มีขอบ = **OOM + latency ระเบิด**. เหตุผลเชิงคณิตศาสตร์: Little's Law
บอกว่าเวลารอ `W = L/λ` ⇒ ถ้า `L` โตไม่จำกัด `W` ก็โตไม่จำกัด
งานที่รอ 10 นาทีมักไม่มีใครต้องการคำตอบแล้ว — **ทำไปก็เสียเปล่า**

| นโยบาย | ทำอะไร | เหมาะกับ |
|---|---|---|
| **block** | ผู้ส่งค้างจนมีที่ว่าง | in-process — backpressure ย้อนขึ้นต้นน้ำจริง (`chan` ทำแบบนี้) |
| **reject** | คืน `ErrFull` / HTTP 429 | ขอบเขต API — ให้ client ตัดสินใจเอง (repo นี้ทำแบบนี้) |
| **drop oldest** | ทิ้งงานเก่าสุด | ข้อมูลที่ค่าเสื่อมตามเวลา — metric, ตำแหน่ง GPS |
| **adaptive LIFO** | ตอนล้น สลับไปทำงานใหม่สุดก่อน | ตอน overload งานหัวคิวมัก timeout ไปแล้ว — ทำก็เสียเปล่า (Facebook ใช้คู่กับ CoDel) |

**ห้ามมีตัวเลือกที่ 5 คือ "โตไปเรื่อย ๆ"**

### 1.6 Visibility timeout (lease) — หัวใจของ at-least-once

`Dequeue` **ไม่ลบ** งาน แต่ *ซ่อน* ไว้ N วินาที. worker ตาย → ไม่มีใคร ack → งานโผล่กลับมาเอง

#### ทำไมถึงเป็นดีไซน์ที่ชนะ

ทางเลือกอื่นคือ **ตรวจจับว่า worker ตาย** ซึ่งในระบบ asynchronous **ทำไม่ได้แบบสมบูรณ์**
(ผลจาก FLP impossibility): แยกไม่ออกระหว่าง "worker ตาย" กับ "worker ช้า/network ช้า"
ต่อให้ทำ heartbeat ก็ยังต้องมี timeout อยู่ดี — และคุณก็กลับมาที่จุดเดิม

lease ยอมรับข้อจำกัดนี้ตรง ๆ แล้วแปลงเป็น **การตัดสินใจฝ่ายเดียวจากนาฬิกาเรือนเดียว**:

```
❌ heartbeat: worker ต้องบอกคิวว่า "ยังอยู่"  → ต้องมี failure detector, ต้อง sync นาฬิกา
✅ lease:     คิวตั้งเวลาเองและตรวจเอง        → นาฬิกาของ "คิว" เป็นความจริงเดียว
```

**ข้อดีที่มักถูกมองข้าม: ไม่ต้อง sync นาฬิกาข้ามเครื่องเลย** เพราะทั้งการตั้ง `leaseUntil`
และการตรวจว่าหมดอายุ เกิดที่คิวที่เดียว — worker จะมีนาฬิกาเพี้ยนแค่ไหนก็ไม่กระทบความถูกต้อง
(ต่างจาก distributed lock ที่นาฬิกาเพี้ยนแล้วพัง)

ราคาที่จ่าย: **งานอาจถูกทำซ้อนกัน** ถ้า worker เก่ายังไม่ตายจริง แค่ช้า → กลับไปที่ [§1.2](#12-delivery-semantics--เรื่องของ-ack-เมื่อไหร่-ล้วน-ๆ):
ต้อง idempotent (หรือใช้ **fencing token** — [§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ))

### 1.7 ความล้มเหลวต้องมีจุดจบ — DLQ

retry ต้องมีเพดาน (`maxAttempt`) แล้วตกลง **DLQ (Dead Letter Queue)**

ไม่งั้น **poison pill** ตัวเดียว (งานที่ทำยังไงก็ล้ม เช่น payload พัง, สินค้าถูกลบไปแล้ว)
จะวนกิน CPU ทั้ง cluster ตลอดกาล และที่แย่กว่าคือมัน **กลบ metric** — คุณจะเห็น error rate สูง
ตลอดเวลาจนแยกไม่ออกว่าอันไหนคือปัญหาจริง

DLQ ไม่ใช่ถังขยะ — เป็น **คิวที่รอมนุษย์ตัดสินใจ**:

```
DLQ → มนุษย์ดู last_error → แก้บั๊ก/แก้ข้อมูล → re-enqueue กลับ ready
                          └→ ข้อมูลเสียจริง    → ลบทิ้งอย่างตั้งใจ
```

---

## 2. ตรรกะ — state machine, invariant, ลำดับเหตุการณ์

### 2.1 ภาพรวมการไหล

```
                                          ┌─ Ack ──▶ ลบทิ้ง (สำเร็จ)
                                          │
  Producer ──Enqueue──▶ ┌───────────┐     │  ┌─ Nack, attempt < max ─▶ DELAYED (backoff)
   (คิวเต็ม = ErrFull)   │   QUEUE   │──▶ Worker
          ▲              └───────────┘   Pool  └─ Nack, attempt ≥ max ─▶ DLQ 🚨
          │                    ▲          │
    backpressure                └─ lease หมดอายุ (worker ตาย) ─┘
    ย้อนไปหา client                  ส่งซ้ำอัตโนมัติ
```

### 2.2 State machine ของงานหนึ่งชิ้น

```
                     Enqueue(RunAt > now)
                  ┌──────────────────────────┐
                  │                          ▼
                  │                     ┌─────────┐
                  │         ┌──────────▶│ DELAYED │
                  │         │ backoff   └────┬────┘
   Enqueue ───────┴─────────┼──────────┐  RunAt ≤ now
                            │          ▼     │
                            │     ┌────────┐◀┘
                            │     │ READY  │◀──────────────┐
                            │     └───┬────┘               │
                            │ Dequeue │ Attempt++          │ leaseUntil < now
                            │         │ lease = now+VT     │ (worker ตาย/ค้าง/GC)
                            │         ▼                    │
                            │   ┌──────────┐───────────────┘
                            │   │ INFLIGHT │
                            │   └─┬──────┬─┘
                            │ Ack │      │ Nack
                            │     ▼      ▼
                            │  ┌──────┐  Attempt < maxAttempt ?
                            │  │ DONE │      │            │
                            │  └──────┘     yes           no
                            └───────────────┘             ▼
                                                       ┌─────┐
                                                       │ DLQ │ ← ตั้ง alert ที่นี่
                                                       └─────┘
```

### 2.3 ตารางการเปลี่ยนสถานะแบบครบถ้วน

ทุกช่องต้องกำหนดไว้ — ช่องที่ "เป็นไปไม่ได้" คือจุดที่บั๊กชอบซ่อน:

| จาก \ เหตุการณ์ | `Enqueue` | `Dequeue` | `Ack` | `Nack` | เวลาผ่านไป |
|---|---|---|---|---|---|
| *(ไม่มี)* | → READY หรือ DELAYED | — | — | — | — |
| **DELAYED** | ⚠️ `ErrFull` ถ้าเต็ม | ข้าม (ยังไม่ถึงเวลา) | ⚠️ `ErrNotInflight` | ⚠️ `ErrNotInflight` | `RunAt ≤ now` → **READY** |
| **READY** | — | → **INFLIGHT** (`Attempt++`) | ⚠️ `ErrNotInflight` | ⚠️ `ErrNotInflight` | — |
| **INFLIGHT** | — | ข้าม (ถูกซ่อนอยู่) | → **DONE** | → DELAYED / **DLQ** | `leaseUntil < now` → **READY** |
| **DLQ** | — | ข้าม | ⚠️ `ErrNotInflight` | ⚠️ `ErrNotInflight` | — |

`⚠️` = คืน error ไม่ใช่ panic ไม่ใช่เงียบ — **การเรียกผิดสถานะต้องส่งเสียง**
เพราะ `ErrNotInflight` คือสัญญาณว่า visibility timeout สั้นเกินไป ([§6.4](#64-งานที่ยาวกว่า-visibility-timeout-จะถูกทำซ้ำตลอดกาล))

### 2.4 Invariant ที่ต้องเป็นจริงตลอดเวลา

| # | Invariant | บังคับใช้อย่างไร | พังแล้วเกิดอะไร |
|---|---|---|---|
| **I1** | งาน 1 ชิ้นอยู่ได้ที่เดียว: `ready ⊎ delayed ⊎ leases ⊎ dead` | ทุกการย้ายทำใต้ `q.mu` เดียวกัน และ `Pop` แล้วค่อย `Push` เสมอ | `Job.index` ชี้ผิด heap → `heap.Remove` **ลบงานอื่นทิ้งเงียบ ๆ** |
| **I2** | `id ∈ inflight` ⟺ `job ∈ leases` | `Ack`/`Nack`/`promoteLocked` แก้ทั้งสองที่ในบล็อกเดียว | งานค้าง inflight ตลอดกาล (memory leak) หรือ lease หมดแล้วไม่มีใครกู้ |
| **I2b** | `Job.ID` ไม่ซ้ำกันในหมู่งานที่ยังไม่จบ | `ids map[string]struct{}` + `ErrDuplicateID` ใน `Enqueue` | **พบบั๊กจริงตอนรีวิว:** ID ซ้ำทำให้ `inflight[id]` ถูกเขียนทับ → `Stats` รายงาน `Inflight=1` ทั้งที่ dequeue ไป 2, `capacity` นับต่ำกว่าจริง, และงานที่ ack ไปแล้วกลับมาถูกทำซ้ำหลัง lease หมด |
| **I2c** | ทุก method ที่แตะสถานะต้องเรียก `promoteLocked()` ก่อน | บรรทัดแรกของ `Dequeue`/`Ack`/`Nack`/`Extend`/`Stats`/`Dead` | **พบตอนรีวิว:** ถ้าไม่ promote `Ack` ที่มาช้า 10 นาทีจะยังสำเร็จ ตราบใดที่ไม่มีใครเรียก `Dequeue` คั่น ⇒ พฤติกรรมขึ้นกับ traffic ที่ไม่เกี่ยวข้อง = ทดสอบไม่ได้ |
| **I3** | `Attempt` เพิ่มที่ **`Dequeue`** เท่านั้น | บรรทัดเดียวใน `Dequeue` | งานที่ทำให้ worker crash จะวนตลอดกาลโดย `Attempt` ไม่ขึ้น → ไม่มีวันถึง DLQ |
| **I4** | `enqueued` ไม่ถูกรีเซ็ตตอน retry | `if j.enqueued.IsZero()` ใน `Enqueue` | `OldestReady` วัดแค่ตั้งแต่ retry รอบล่าสุด → **มองไม่เห็นงานที่ค้างมา 3 ชั่วโมง** |
| **I5** | ไม่ถือ mutex ขณะเรียก handler | `Dequeue` `Unlock` ก่อน `return` | คิวทั้งใบหยุดตามเวลาที่ handler ใช้ = concurrency กลายเป็น 1 |
| **I6** | `capacity` นับรวม `inflight` ด้วย | `sizeLocked()` | รับงานเข้ามาเกินที่ระบบทำไหว → ย้อนไปข้อ [§1.5](#15-bounded-เสมอ--และ-4-นโยบายเมื่อเต็ม) |

> **I3 มีชื่อในของจริง:** SQS เรียก `ApproximateReceiveCount` และใช้ตัดสิน redrive policy
> ด้วยเหตุผลเดียวกันเป๊ะ ๆ

### 2.5 ลำดับเหตุการณ์: at-least-once ตอน worker ตาย

```
 Worker A          Queue                              Worker B
    │                │  ready=[j7]
    │──Dequeue──────▶│  Attempt=1, lease=T+30s, inflight={j7}
    │◀───── j7 ──────│
    │                │
   ทำงาน...          │
   💥 ตาย            │
    ✗                │
                     │  t=T+30s  promoteLocked():
                     │    leaseUntil ≤ now → delete(inflight, j7)
                     │    j7 กลับเข้า ready
                     │◀────────────────────Dequeue──────│
                     │  Attempt=2, lease=T+60s ────j7──▶│
                     │                                  │ ทำงาน (ซ้ำครั้งที่ 2!)
                     │                                  │ ← ต้อง idempotent ตรงนี้
                     │◀───────────Ack(j7)───────────────│
                     │  delete(inflight), heap.Remove(leases)
```

**สังเกต:** ไม่มีใคร "แจ้ง" ว่า A ตาย ไม่มี failure detector ไม่มี timeout ฝั่ง worker
— แค่ `promoteLocked` เห็นว่า `leaseUntil` ผ่านมาแล้วตอนที่ B มาขอ

### 2.6 ปัญหาที่แก้ไม่ได้: Ack แข่งกับ lease หมดอายุ

```
 t=29.99  worker A ทำงานเสร็จ กำลังจะเรียก Ack
 t=30.00  lease หมด → คิวปล่อย j7 กลับ ready
 t=30.01  worker A เรียก Ack → ErrNotInflight (สายไป 10ms)
 t=30.02  worker B รับ j7 ไปทำซ้ำ
```

**ไม่มีวิธีปิดช่องนี้ด้วยการปรับ timeout** — ทำได้แค่ทำให้แคบลง ช่องนี้คือเหตุผลที่
[§1.2](#12-delivery-semantics--เรื่องของ-ack-เมื่อไหร่-ล้วน-ๆ) บอกว่า exactly-once ไม่มีจริง

**สองวิธีรับมือ:**

**(ก) Idempotency** — ทำซ้ำแล้วผลเหมือนเดิม ([§6.3](#63-idempotency-ไม่ใช่-nice-to-have))

**(ข) Fencing token** — ใช้ `Attempt` เป็นเลขลำดับที่โตขึ้นเรื่อย ๆ แล้วให้ปลายทาง
**ปฏิเสธ write ที่มาจาก attempt ต่ำกว่าที่เคยเห็น**:

```sql
-- worker เก่า (attempt=1) ที่ฟื้นมาช้า จะแก้ข้อมูลไม่ได้แล้ว
UPDATE invoices
SET status = 'paid', fence = $2      -- $2 = j.Attempt
WHERE id = $1 AND fence < $2;        -- ★ กันการเขียนย้อนหลังจาก worker ที่ตกยุค
```

วิธีนี้แก้ปัญหาที่ idempotency แก้ไม่ได้: กรณีที่ worker เก่า **เขียนทับ**
ผลลัพธ์ที่ใหม่กว่าของ worker B (เช่น A กำลังจะเขียน `status='processing'`
หลังจาก B เขียน `status='paid'` ไปแล้ว)

### 2.7 ทำไมต้องมี `Nack` ทั้งที่ lease หมดเองได้

`Nack` ไม่จำเป็นเชิงความถูกต้อง — ถ้าไม่มี งานที่ล้มก็จะกลับมาเองเมื่อ lease หมด

`Nack` เป็น **การปรับให้เร็วขึ้น (optimization)** สองอย่าง:

1. **คืนงานทันที** ไม่ต้องรอ VT (30 วินาที) → latency ดีขึ้นมาก
2. **คุม backoff ได้** — lease หมดเองจะกลับ ready ทันที (delay = 0)
   ซึ่งผิดสำหรับความล้มเหลวที่รู้สาเหตุ (DB ล่ม → ต้อง backoff)

นี่คือเหตุผลที่ `promoteLocked` เรียก `retryLocked(j, 0)` (ไม่ backoff)
แต่ `Nack` เรียก `retryLocked(j, delay)` — **ความล้มเหลวที่ "รู้ตัว" ควรรอ
ส่วนความล้มเหลวที่ "ตายไปเลย" ควรกลับมาทันที**

### 2.8 กฎที่ห้ามละเมิด

1. **Ack หลังทำงานสำเร็จเท่านั้น** — ack ก่อนคือ at-most-once ไม่ว่าจะตั้งใจหรือไม่
2. **handler ต้อง idempotent** — ไม่มีข้อยกเว้น ([§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ))
3. **handler ต้องเคารพ `ctx`** — ไม่งั้น graceful shutdown กลายเป็น hard kill
4. **panic ในงานหนึ่งชิ้นห้ามล้มทั้งโปรเซส** — `recover` ต่องาน ([§4.6](#46-worker-pool--panic-guard--graceful-shutdown))
5. **retry ต้องมี jitter** — ไม่งั้น thundering herd ([§4.5](#45-exponential-backoff--full-jitter))
6. **อย่าเก็บ state ใน worker** — worker ต้องแทนที่กันได้ทุกตัว ไม่งั้น lease redelivery ไร้ความหมาย

---

## 3. โครงสร้าง — โมดูล, ชนิดข้อมูล, หน่วยความจำ

### 3.1 การแบ่งไฟล์เมื่อโตเป็นของจริง

```
internal/queue/
├── job.go        Job, sentinel errors                  (~40 บรรทัด)
├── queue.go      interface Queue                       (~15)
├── mem.go        MemQueue: 3 heap + lease              (~200)  ← dev/test/งานที่หายได้
├── pg.go         PgQueue: SKIP LOCKED                  (~150)  ← production
├── worker.go     RunPool, Backoff, panic guard         (~60)
└── metrics.go    prometheus collectors                 (~40)
```

repo นี้ยุบเหลือ `queue.go` ไฟล์เดียวเพราะยังมี implementation เดียว
— **แตกไฟล์เมื่อมีตัวที่สอง ไม่ใช่ก่อนหน้านั้น**

### 3.2 Interface — ประกาศเมื่อมี implementation ที่สองจริง ๆ

```go
type Queue interface {
	Enqueue(ctx context.Context, j *Job) error
	Dequeue(ctx context.Context) (*Job, error)                 // บล็อก; lease เริ่มนับทันที
	Ack(ctx context.Context, id string) error
	Nack(ctx context.Context, id string, after time.Duration, cause error) error
	Extend(ctx context.Context, id string, d time.Duration) error
}
```

`MemQueue` กับ `PgQueue` ใช้ semantics เดียวกัน ⇒ ย้ายจากหน่วยความจำไป Postgres
**โดยไม่แตะ handler เลยแม้แต่บรรทัดเดียว** — นี่คือมูลค่าจริงของ interface นี้
ไม่ใช่ "ความยืดหยุ่น" แบบลอย ๆ

> **สองกฎของ interface ใน Go:**
> 1. **ประกาศที่ฝั่งผู้ใช้ ไม่ใช่ฝั่งผู้ให้บริการ** — package ที่ *ใช้* คิวเป็นคนนิยามว่าต้องการอะไร
> 2. **ถ้ามี implementation เดียว อย่าใส่ interface** — ได้แค่ indirection
>    ที่ทำให้ jump-to-definition ใน IDE พาไปผิดที่ และ mock ที่ไม่มีใครใช้

### 3.3 โครงสร้างข้อมูลในหน่วยความจำ

```
┌──────────────────── MemQueue (mutex ตัวเดียวคุมทุกอย่าง) ─────────────────────┐
│                                                                              │
│   delayed  (min-heap by RunAt)            ready  (max-heap by Priority)      │
│   ┌──────────────────────────┐  RunAt≤now  ┌────────────────────────────┐    │
│   │ t+1s │ t+5s │ t+30s │... │────────────▶│ p=9,seq3 │ p=9,seq7 │ p=0 │ │    │
│   └──────────────────────────┘  promote()  └────────────────────────────┘    │
│              ▲                                            │ Dequeue()        │
│              │ Nack + backoff                             │ Attempt++        │
│              │                                            ▼ lease=now+VT     │
│   ┌──────────┴───────────────┐              ┌────────────────────────────┐   │
│   │ lease หมด → promote กลับ  │◀─────────────│ leases (min-heap by        │   │
│   └──────────────────────────┘              │         leaseUntil)        │   │
│                                              │ inflight map[string]*Job   │   │
│   dead []*Job  ── DLQ                        └────────────────────────────┘   │
│                                                                              │
│   wake chan struct{}   ← condition variable ที่ select() ได้                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### ทำไม 3 heap ไม่ใช่ 1

comparator ที่ผสม *เวลา* กับ *ความสำคัญ* จะ **ไม่ transitive**:

```
สมมติ less(a,b) = "a ถึงเวลาแล้วแต่ b ยัง"  หรือ  "ทั้งคู่ถึงเวลาแล้วและ a.Priority สูงกว่า"

a: RunAt=t+0, Priority=1      →  less(a,b)? a ถึงเวลา b ยัง          → true
b: RunAt=t+5, Priority=9      →  less(b,c)? ทั้งคู่ยังไม่ถึง... 
c: RunAt=t+9, Priority=5      

เมื่อเวลาเดินไป 5 วินาที ผลของ less(a,b) เปลี่ยน — แต่ heap ไม่รู้
⇒ โครงสร้าง heap ที่สร้างไว้ตอน t=0 ผิดตั้งแต่ t=5 โดยไม่มีใครแจ้ง
```

**comparator ที่ผลลัพธ์เปลี่ยนตามเวลา ทำลาย heap invariant เงียบ ๆ**
— แยกเป็น 3 heap ตามมิติที่เรียง แต่ละตัวมี comparator ที่คงที่ = ถูกและอ่านออก

#### ทำไมเก็บ `*Job` ไม่ใช่ `Job`

1. **ค่าใช้จ่ายในการ sift** — `Job` ใหญ่ ~120 ไบต์ การ `Swap` ระหว่าง sift-up/down
   จะคัดลอกทั้งก้อน `O(log n)` ครั้งต่อการ push. pointer = 8 ไบต์
2. **identity สำคัญ** — `Job.index` ต้องเป็นของ instance เดียวกันที่อยู่ในทั้ง
   `inflight` map และใน heap. ถ้าเก็บเป็น value จะกลายเป็นคนละสำเนาทันที
3. **`inflight` map ชี้ไปที่ตัวเดียวกัน** — แก้ `Attempt` ที่เดียวเห็นทุกที่

ข้อเสีย: GC ต้องไล่ pointer มากขึ้น. บรรเทาด้วย `h.jobs[n-1] = nil` ใน `Pop`
เพื่อไม่ให้ backing array ถือ pointer ค้างหลังงานถูกเอาออกไปแล้ว

#### ทำไมต้องมี `Job.index` (และทางเลือกที่ไม่เลือก)

`Ack` ต้องเอางานออกจาก `leases` heap ทันที ซึ่งต้องรู้ตำแหน่ง:

| ทางเลือก | ต้นทุน | ทำไมไม่เลือก |
|---|---|---|
| **`index` + `heap.Remove`** ✅ | `O(log n)`, เพิ่ม 4 บรรทัดใน `Swap`/`Push`/`Pop` | — |
| ค้นเชิงเส้นแล้ว `Remove` | `O(n)` ต่อ Ack | Ack คือ hot path — เกิดทุกงาน |
| **lazy deletion** (ปล่อยค้างใน heap แล้วข้ามตอน pop) | `O(1)` Ack แต่ heap บวม | ที่ 1,000 job/s + VT 30s → **30,000 entry ขยะค้างตลอดเวลา** |

เลือก `index` เพราะ **จ่ายโค้ดไม่กี่บรรทัดแล้วได้ความถูกต้องบน edge case**
— นี่คือกรณีที่ "ขี้เกียจ" แปลว่าเขียนน้อย ไม่ใช่เลือกอัลกอริทึมที่เปราะกว่า

#### ทำไม `chan` ไม่พอ

`chan T` คือ ring buffer + waiter queue ที่ runtime ให้มาแล้ว:

```
ch := make(chan Job, 4)        runtime.hchan
        ┌────────────────────────────────────────────┐
        │ lock mutex     qcount=3   dataqsiz=4       │
        │ buf ▶ [ j1 ][ j2 ][ j3 ][    ]             │  ← ring buffer
        │         ▲                 ▲                │
        │      recvx=0           sendx=3             │
        │ sendq: goroutine ที่รอ send (คิวเต็ม)       │  ← backpressure ฟรี
        │ recvq: goroutine ที่รอ recv (คิวว่าง)       │  ← blocking dequeue ฟรี
        │ elemtype, elemsize, closed                 │
        └────────────────────────────────────────────┘
```

หนึ่งบรรทัดได้ ring buffer + mutex + condition variable + backpressure

**`chan` ให้ไม่ได้ 5 อย่าง — และนี่คือเหตุผลเดียวที่ควรเขียนเพิ่ม:**

| ขาด | ทำไมสำคัญ |
|---|---|
| **priority** | งานด่วนต้องแซงได้ |
| **delay** | retry ต้องรอ, scheduled job ต้องรอ |
| **ack/lease** | consumer ตาย = งานหาย (channel ลบทันทีที่รับ) |
| **persistence** | process ตาย = หายหมด |
| **การมองเห็นสถานะ** | `len(ch)` บอกแค่ตัวเลข ไม่บอกว่างานเก่าสุดรอมากี่นาที |

> **เกร็ด `select`:** runtime สุ่มลำดับตรวจ case (`pollorder`) เพื่อความยุติธรรม
> และ**ล็อก channel ทุกตัวเรียงตามที่อยู่ในหน่วยความจำ** (`lockorder`) เพื่อกัน deadlock
> — เหตุผลที่ `select` หลาย channel ไม่ทำให้เกิด lock-order inversion

### 3.4 API ที่เปิดออก

| ฟังก์ชัน | สัญญา | ข้อควรระวัง |
|---|---|---|
| `NewMemQueue(capacity, maxAttempt, visibility)` | สร้างคิว | `visibility` **ต้อง > p99.9 ของเวลาทำงานจริง** |
| `Enqueue(*Job) error` | ไม่บล็อก | `ErrFull` / `ErrClosed` / **`ErrDuplicateID`**; **ห้ามแก้ `*Job` หลังส่งเข้าไป** |
| `Dequeue(ctx) (*Job, error)` | บล็อก | lease เริ่มนับ **ทันที** ที่คืนค่า ไม่ใช่ตอนเริ่มทำงาน |
| `Ack(id) error` | ยืนยันสำเร็จ | `ErrNotInflight` = lease หมดไปแล้ว → นับใน `Stats.AckTooLate` ให้อัตโนมัติ |
| **`Extend(id, d) error`** | ต่ออายุ lease | เทียบเท่า `ChangeMessageVisibility`; ล้มเหลว = **หยุดทำงานทันที** |
| `Nack(id, delay, cause) error` | แจ้งล้มเหลว | `delay` จาก `Backoff(j.Attempt)`; `cause` → `Job.LastErr` ให้ DLQ วินิจฉัยได้ |
| `Close()` | หยุดรับงานใหม่ | งานใน `delayed` **ถูกทิ้ง** |
| `Stats() Stats` | อ่านสถานะ | `O(n)`; **มี side effect โดยตั้งใจ** — promote ก่อนนับ |
| `Dead() []*Job` | สำเนา**ลึก**ของ DLQ | **มี side effect โดยตั้งใจ** — promote ก่อนอ่าน ([I2c](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)); ตัวที่ได้ส่งกลับเข้า `Enqueue` ได้เลย |
| `RunPool(ctx, q, workers, timeout, h)` | บล็อกจน worker ออกครบ | เรียกใน goroutine ถ้าต้องการ drain แบบมี deadline |
| `Backoff(attempt) time.Duration` | exponential + full jitter | คืนค่าได้ใกล้ 0 — เป็นเรื่องปกติของ full jitter |

**กฎเดียวที่ทำให้ semantics คาดเดาได้: ทุก method ที่แตะสถานะจะ `promoteLocked()` ก่อนเสมอ**
(`Dequeue`, `Ack`, `Nack`, `Extend`, `Stats`, `Dead`) — ไม่งั้นการตัดสินว่า "lease หมดหรือยัง"
จะขึ้นกับว่า *มี goroutine อื่นบังเอิญเรียก `Dequeue` คั่นหรือไม่* ซึ่งไม่ deterministic
ราคาคือ peek สอง heap = `O(1)` เมื่อไม่มีอะไรหมดอายุ

### 3.5 ตัวเลขที่วัดได้จริง (Apple M4, 10 cores, `-benchmem`)

| สถานการณ์ | ns/op | throughput | allocs/op |
|---|---|---|---|
| `chan` เปล่า 8 worker (เส้นฐาน) | 264 | ~3.8M job/s | 2 |
| MemQueue 1 worker | 583 | ~1.7M job/s | 5 |
| MemQueue 8 worker | 1,170 | **~855k job/s** | 5 |
| MemQueue 64 worker | 3,310 | ~302k job/s | 6 |
| MemQueue 512 worker | 13,461 | ~74k job/s | 6 |

**อ่านตัวเลขนี้ยังไง:**

1. **ราคาของ feature ทั้งชุด ≈ 4.4×** เทียบกับ `chan` เปล่าที่ 8 worker
   — ถ้าไม่ต้องการ priority/delay/lease/DLQ ก็อย่าจ่าย ([§0](#0-เลือกก่อนเขียนโค้ด))
2. **throughput ตกตามจำนวน worker** เพราะ broadcast ปลุกทุกคนเพื่องานชิ้นเดียว
   ([§4.3](#43-condition-variable-ที่-select-ได้--อัลกอริทึมสำคัญที่สุดในไฟล์)) — ที่ 8 worker ยังสบาย ที่ 512 เหลือ 1/11
3. **allocs/op เกือบคงที่ทุกจำนวน worker** — เพราะ `Dequeue` ใช้ `time.Timer` ตัวเดียวแล้ว `Reset`
   ก่อนแก้จุดนี้ ตัวเลขคือ **31 allocs/op และ 2,527 B/op ที่ 512 worker**
   (timer ที่ waiter สร้างใหม่ทุกครั้งที่ถูกปลุกแล้วกลับไปนอน)
4. **1 alloc ในนั้นคือสำเนา `Job` ที่ `Dequeue` คืน** (~3% ของ ns/op, +160 B/op) —
   ราคาของการไม่แชร์ `*Job` กับ worker ([§3.6](#36-ขอบเขตความเป็นเจ้าของ-ทำไม-dequeue-ต้องคืนสำเนา))
   ตอนถือ pointer ตัวจริงคือ **4 allocs/op และ 1,170 → 1,132 ns/op** — เร็วขึ้น 3% แลกกับ data race

⇒ **ใช้ได้จริงถึงหลักแสน job/s ที่ worker ไม่เกินหลักสิบ** ซึ่งครอบคลุมเกือบทุกงานจริง
เกินกว่านั้นให้ shard ([§7](#7-รีวิว-ข้อจำกัดที่รู้ตัว) ข้อ 1) ไม่ใช่ปรับจูนอันนี้

### 3.6 ขอบเขตความเป็นเจ้าของ: ทำไม `Dequeue` ต้องคืน**สำเนา**

นี่คือกฎที่ตามมาจาก at-least-once โดยตรง และเป็นจุดที่พลาดง่ายที่สุดในไฟล์:

```
t0  worker A: j := Dequeue()      → คิวคืน *Job ตัวที่ตัวเองยังถืออยู่ใน inflight/leases
t1  lease หมด: promoteLocked() ยัด **pointer ตัวเดิม** กลับเข้า ready
t2  worker B: Dequeue() ได้ตัวเดิม → j.Attempt++  (เขียน ใต้ q.mu)
t3  worker A: ทำงานเสร็จ → Backoff(j.Attempt)     (อ่าน นอก q.mu)   ← DATA RACE
```

**คนที่อ่านคือ `RunPool` เอง** ไม่ใช่ผู้ใช้ที่เขียนโค้ดผิด — `q.Nack(j.ID, Backoff(j.Attempt), err)`
ประเมิน argument ก่อนเข้าล็อก. ห้ามด้วยเอกสารไม่ได้ ต้องห้ามด้วยชนิดข้อมูล

**ทางแก้:** `cp := *j` ใต้ล็อกเดียวกับที่ตั้ง lease แล้วคืน `&cp`
คิวเก็บตัวจริงไว้คนเดียว ผู้เรียกได้ snapshot ที่ไม่มีใครเขียนอีกเลย

| ทางเลือก | ทำไมไม่เอา |
|---|---|
| ใส่ `sync.Mutex` ใน `Job` | ล็อกต่องาน + ผู้ใช้ต้องรู้ว่าต้องล็อกก่อนอ่าน = API ที่ใช้ผิดได้ |
| ใส่ `atomic.Int64` ที่ `Attempt` | แก้ได้ฟิลด์เดียว `LastErr`/`RunAt` ยังแข่งกันอยู่ |
| เอกสารว่า "ห้ามอ่าน `j.Attempt`" | `RunPool` ในไฟล์เดียวกันก็ยังอ่าน — เอกสารไม่บังคับใคร |
| **คืนสำเนา** | +1 alloc, +3% ns/op, ไม่มีทางใช้ผิด ✅ |

หลักการเดียวกันใช้กับ `Dead()` — คืนสำเนาลึก ไม่งั้นการ replay
(`q.Enqueue(q.Dead()[0])`) จะทำให้ `*Job` ตัวเดียวอยู่ทั้งใน DLQ และในคิวพร้อมกัน

---

## 4. อัลกอริทึม — 6 ตัวพร้อม complexity

| การดำเนินการ | Complexity | หมายเหตุ |
|---|---|---|
| `Enqueue` | `O(log n)` | heap sift-up |
| `Dequeue` | `O((k+1) log n)` | `k` = จำนวนงานที่ promote ในรอบนั้น; amortized `O(log n)` เพราะแต่ละงาน promote ได้ครั้งเดียวต่อรอบชีวิต |
| `Ack` / `Nack` | `O(log n)` | `heap.Remove` ด้วย `index` |
| `Stats` | `O(n)` | ตั้งใจ — เรียกทุก 10 วินาที ไม่ใช่ hot path |
| หน่วยความจำ | `O(n)` | 1 pointer ต่องาน ไม่มีสำเนา |

### 4.1 Priority + FIFO — binary heap ที่มี tie-break

#### heap ทำงานอย่างไร (ทบทวนแบบเห็นภาพ)

binary heap คือ array ที่แทน complete binary tree — ลูกของ `i` คือ `2i+1`, `2i+2`:

```
array:  [ 9 │ 7 │ 5 │ 3 │ 6 │ 1 ]        tree:        9
index:    0   1   2   3   4   5                     ╱   ╲
                                                   7     5
Push(8):  ต่อท้าย → sift-up จนกว่าพ่อจะไม่เล็กกว่า  ╱ ╲   ╱
  [9,7,5,3,6,1,8] → 8 > 5 สลับ → [9,7,8,3,6,1,5]  3   6 1

Pop():    สลับ root ↔ ท้าย → ตัดท้ายออก → sift-down จาก root
```

`heap.Push` = append แล้ว `up(n-1)`; `heap.Pop` = `swap(0, n-1)`, `down(0, n-1)`, แล้วเรียก `h.Pop()`
— **นี่คือเหตุผลที่ `h.Pop()` ต้องคืน element ท้าย slice** ไม่ใช่ตัวแรก

#### ปัญหา: heap ให้ partial order เท่านั้น

`container/heap` การันตีแค่ "root คือตัวที่เล็กที่สุดตาม `Less`" — **ไม่การันตีลำดับ
ของ element ที่ `Less` เท่ากัน** เพราะการ sift สลับตำแหน่งไปเรื่อย ๆ

⇒ งาน priority เท่ากันจะออกมาสลับกันมั่ว ผู้ใช้เห็นเป็น "งานที่ส่งทีหลังทำก่อน" แบบสุ่ม

#### วิธีแก้: sequence number

```go
ready: jobHeap{less: func(a, b *Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.seq < b.seq   // ← ทำให้เป็น total order + FIFO ภายในชั้นเดียวกัน
}}
```

`seq` เพิ่มขึ้นทีละ 1 ใต้ล็อกใน `Enqueue` ⇒ ไม่มีทางซ้ำ ⇒ comparator เป็น **total order**
(ไม่มีคู่ไหน "เท่ากัน") ⇒ heap เรียงได้ deterministic

`uint64` ล้นเมื่อไหร่? ที่ 1 ล้าน job/s ต้องใช้ **~584,000 ปี** — ไม่ต้องกังวล

#### สองจุดที่พลาดบ่อยกับ `container/heap`

1. **`Pop()` ต้องคืน element ท้าย slice** — ถ้าเขียน `return h.jobs[0]` จะได้ค่าผิด
   และ heap พังทันทีโดยไม่ error
2. **`Swap()` ต้องอัปเดต `index` ของทั้งคู่** — ลืมแล้ว `heap.Remove(h, j.index)`
   จะลบงานอื่นทิ้ง **เงียบ ๆ** ไม่มี panic ไม่มี log — บั๊กประเภทที่ใช้เวลาหาเป็นวัน

> **`heap.Fix` vs `heap.Remove`:** ถ้าแค่ *เปลี่ยนค่า* ของ element ที่อยู่ใน heap แล้ว
> (เช่น เลื่อน priority) ใช้ `heap.Fix(h, i)` — `O(log n)` เหมือนกัน แต่ไม่ต้อง Remove+Push
> repo นี้ไม่ใช้เพราะไม่มีการแก้ค่าในที่

### 4.2 Delay queue — min-heap + timer **ตัวเดียว**

#### ทางที่ผิด

```go
// ❌ 100k งาน = 100k goroutine + 100k timer
for _, j := range jobs {
	go func(j *Job) { time.Sleep(until(j.RunAt)); enqueue(j) }(j)
}
```

goroutine ละ ~8KB stack เริ่มต้น → 100k = 800MB. และ scheduler ต้องดูแลทั้งหมด

#### ทางที่ถูก: heap + timer ที่ head เท่านั้น

งานที่จะ "ถึงเวลา" ก่อนเสมอคือ head ของ min-heap ⇒ ตั้ง timer แค่ตัวเดียว
และเมื่อ timer ดัง ค่อยคำนวณ event ถัดไปใหม่

ยิ่งกว่านั้น — **delay กับ lease-expiry รวมเป็น timeline เดียวได้**:

```go
func (q *MemQueue) nextEventLocked(now time.Time) (time.Duration, bool) {
	var t time.Time
	if h := q.delayed.peek(); h != nil {
		t = h.RunAt                    // งาน delayed ที่ใกล้ถึงเวลาที่สุด
	}
	if h := q.leases.peek(); h != nil && (t.IsZero() || h.leaseUntil.Before(t)) {
		t = h.leaseUntil               // lease ที่ใกล้หมดอายุที่สุด — เอาตัวที่มาก่อน
	}
	if t.IsZero() {
		return 0, false                // ไม่มี event → caller ใช้ nil channel = บล็อกตลอดกาล
	}
	return max(t.Sub(now), 0), true    // max(...,0) กันค่าติดลบเมื่อ event เลยมาแล้ว
}
```

**`nil` channel ใน `select` บล็อกตลอดกาล** — เป็น idiom ของ Go ที่ใช้ "ปิด" case
โดยไม่ต้องเขียน `if` สองชั้น:

```go
var timerC <-chan time.Time     // nil
if has {
	timerC = time.NewTimer(d).C  // ไม่ nil ก็ต่อเมื่อมี event จริง
}
select {
case <-timerC:  // ถ้า timerC เป็น nil case นี้จะไม่มีวันถูกเลือก
case <-wake:
case <-ctx.Done():
}
```

#### เปรียบเทียบทางเลือก

| โครงสร้าง | Insert | Delete-min | เหมาะเมื่อ |
|---|---|---|---|
| sorted slice | `O(n)` | `O(1)` | timer < ~100 ตัว, แทบไม่ insert |
| **binary heap** ✅ | `O(log n)` | `O(log n)` | ทั่วไป — **เลือกอันนี้** |
| hierarchical timing wheel | `O(1)` | `O(1)` amortized | timer **หลักล้าน**, ยอมให้ความละเอียดเป็น tick |

timing wheel คือ array วงกลมของ bucket แต่ละช่องเก็บงานที่จะดังในช่วงเวลานั้น
ซ้อนหลายชั้นเหมือนเข็มนาฬิกา (วินาที/นาที/ชั่วโมง) — Kafka และ Linux kernel ใช้:

```
ชั้น 1 (ทุก 1ms, 256 ช่อง = 256ms)   [·][·][X][·][·]...[·]
                                            ▲ cursor
ชั้น 2 (ทุก 256ms, 64 ช่อง = 16s)     [·][XX][·]...        ← ล้นจากชั้น 1 มาที่นี่
ชั้น 3 (ทุก 16s, 64 ช่อง = 17 นาที)   [X][·][·]...
```

**ต่ำกว่าหลักล้าน timer — heap ชนะขาดเรื่องความเรียบง่าย** (heap = 30 บรรทัด, wheel = 300)

> **Go ≥ 1.23:** `timer.C` เป็น **unbuffered** แล้ว — หลัง `Stop()`/`Reset()` คืนค่า
> จะไม่มีค่าค้างหลุดออกมาอีก. ท่า *drain channel ก่อน Reset* ที่เขียนกันมาสิบปีเลิกใช้ได้
> และ timer ที่ไม่ได้ `Stop` ก็ถูก GC เก็บได้แล้ว (ก่อนหน้านี้ค้างจนกว่าจะดัง)

### 4.3 Condition variable ที่ `select` ได้ — อัลกอริทึมสำคัญที่สุดในไฟล์

#### ทำไม `sync.Cond` ใช้ไม่ได้

```go
// ❌ ยกเลิกไม่ได้
for q.ready.Len() == 0 {
	q.cond.Wait()          // ไม่มี ctx, ไม่มี timeout, ไม่มีทางออก
}
```

`Cond.Wait()` **ไม่รับ `context` และ `select` ไม่ได้** ⇒ ตอน shutdown goroutine
ที่ค้างอยู่ตรงนี้จะไม่มีวันออก. Go team เองก็แนะนำให้ใช้ channel แทนในกรณีส่วนใหญ่

#### ท่าที่ถูก: channel ที่ปิดแล้วสร้างใหม่

```go
// ── ผู้ปลุก (ถือล็อกอยู่) ──
func (q *MemQueue) broadcastLocked() {
	close(q.wake)              // ★ ปิด channel = ปลุก waiter *ทุกตัว* พร้อมกัน
	q.wake = make(chan struct{})
}

// ── ผู้รอ ──
q.mu.Lock()
/* ...เช็กเงื่อนไข: มีงานไหม? ปิดหรือยัง?... */
wake := q.wake                 // ★ จับ ref ใต้ล็อกเดียวกับผู้ปลุก
q.mu.Unlock()
select {
case <-wake:                   // มีงานใหม่ / มีคน nack / มีการ promote
case <-timerC:                 // ถึงเวลา delayed หรือ lease หมดอายุ
case <-ctx.Done():             // ★ ยกเลิกได้จริง — sync.Cond ทำข้อนี้ไม่ได้
	return nil, ctx.Err()
}
```

#### พิสูจน์ว่าไม่มี missed wakeup

ให้ `t_capture` = เวลาที่ผู้รอทำ `wake := q.wake`, `t_select` = เวลาที่เข้า `select`,
`t_close` = เวลาที่ผู้ปลุกทำ `close(q.wake)`

ทั้ง `t_capture` และ `t_close` เกิดใต้ mutex ตัวเดียวกัน ⇒ **เรียงลำดับกันเสมอ** ⇒ มีแค่ 2 กรณี:

| กรณี | เกิดอะไร |
|---|---|
| `t_close < t_capture` | ผู้ปลุกปิด channel เก่าและสร้างใหม่ไปแล้ว. ผู้รอจับ channel *ใหม่* — แต่ผู้รอก็เช็กเงื่อนไขใต้ล็อกเดียวกัน**หลัง**การเปลี่ยนสถานะ ⇒ **เห็นงานแล้ว ไม่เข้า select เลย** |
| `t_capture < t_close` | ผู้รอถือ channel ที่ยังไม่ปิด. เมื่อผู้ปลุกปิด **channel ตัวนั้นเอง** ⇒ `select` ผ่านทันที ไม่ว่า `t_close` จะอยู่ก่อนหรือหลัง `t_select` |

**ไม่มีช่องว่าง** เพราะการเช็กเงื่อนไขกับการจับ `wake` เป็น atomic เมื่อเทียบกับผู้ปลุก

#### ทางเลือกที่มีบั๊ก (อย่าทำ)

```go
// ❌ buffered chan cap 1 + non-blocking send
notify := make(chan struct{}, 1)
select { case notify <- struct{}{}: default: }   // "signal"
```

**บั๊ก:** Enqueue 2 งานติดกัน → ส่งสัญญาณได้แค่ 1 (ตัวที่สอง `default` ทิ้งไป)
→ waiter ตื่นแค่ 1 ตัว → **งานที่สองค้างจนกว่าจะมี event อื่น**
แก้ได้ด้วย "signal chaining" (ตื่นแล้วส่งต่อถ้ายังมีงาน) แต่นั่นคือเพิ่มความซับซ้อน
เพื่อแก้ปัญหาที่ `close` ไม่มีตั้งแต่แรก

#### ทำไมต้องมี `for` ครอบ

broadcast ปลุก N ตัวเพื่องาน 1 ชิ้น — **spurious wakeup by design**
ตัวที่แพ้ต้องวนไปเช็กใหม่ หลักการเดียวกับ `while (!cond) pthread_cond_wait()`

**ต้นทุน:** N worker ตื่นพร้อมกันแย่ง mutex เพื่องาน 1 ชิ้น = `O(N)` context switch เสียเปล่า
ที่ N ≤ ~100 ไม่มีนัยสำคัญ; ที่ N > 1,000 ควรเปลี่ยนไปทำ waiter queue แบบ FIFO
(ให้ waiter แต่ละตัวมี channel ของตัวเอง แล้วปลุกทีละตัว — ซึ่งคือสิ่งที่ `hchan.recvq` ทำ)

### 4.4 Lease / visibility timeout — ไม่ต้องมี reaper goroutine

ตำราจะบอกให้ตั้ง goroutine เดินตรวจ lease ทุก 1 วินาที **ไม่ต้อง** — ตรวจตอน `Dequeue` เลย:

```go
func (q *MemQueue) promoteLocked(now time.Time) {
	moved := false
	for h := q.delayed.peek(); h != nil && !h.RunAt.After(now); h = q.delayed.peek() {
		heap.Push(&q.ready, heap.Pop(&q.delayed).(*Job))          // delayed → ready
		moved = true
	}
	for h := q.leases.peek(); h != nil && !h.leaseUntil.After(now); h = q.leases.peek() {
		j := heap.Pop(&q.leases).(*Job)
		delete(q.inflight, j.ID)                                  // รักษา I2
		q.retryLocked(j, 0)          // worker ตาย → ส่งซ้ำทันที (Attempt นับไปแล้ว)
		moved = true
	}
	if moved {
		q.broadcastLocked()          // ปลุก waiter ตัวอื่นที่รออยู่ด้วย
	}
}
```

#### เหตุผลเชิงตรรกะ

> **ถ้าไม่มีใครเรียก `Dequeue` ก็ไม่มีใครเดือดร้อนว่า lease หมด**

งานจะถูกกู้ตอนมีคนมาขอ ซึ่งเป็นเวลาเดียวที่มีความหมาย. ได้ผลตอบแทน 3 อย่าง:

1. **ประหยัด 1 goroutine + 1 timer** ตลอดอายุโปรเซส
2. **ตัด race ทั้งคลาสทิ้ง** — ไม่มี reaper ที่แข่งกับ `Ack` แย่งงานตัวเดียวกัน
3. **ไม่ต้องจัดการ lifecycle ของ reaper** — ไม่ต้องหยุดมันตอน `Close()`

#### ทำไม `!h.leaseUntil.After(now)` ไม่ใช่ `h.leaseUntil.Before(now)`

`!After(now)` คือ `≤ now` ส่วน `Before(now)` คือ `< now`

ที่ความละเอียดนาโนวินาทีดูเหมือนไม่ต่าง — แต่ใน `testing/synctest` ที่นาฬิกาเดินเป็นก้าว
เวลาจะ **เท่ากันเป๊ะ** ได้บ่อย ⇒ `Before` จะทำให้งานค้างไปอีกหนึ่งรอบ
**เงื่อนไขขอบต้องเลือกให้ inclusive เสมอสำหรับ deadline**

#### ความแตกต่างกับ Postgres

ใน Postgres **ต้องมี reaper จริง** เพราะ DB ไม่มี "ตอนที่ถูกเรียก" ที่จะแอบทำงานได้
— และ worker หลายเครื่องต่างคนต่างมี `Dequeue` ของตัวเอง ไม่มีจุดรวม ([§5.2](#52-dequeue--atomic-worker-กี่ตัวก็ไม่ชนกัน))

### 4.5 Exponential backoff + full jitter

```go
func Backoff(attempt int) time.Duration {
	const base, ceiling = 100 * time.Millisecond, 30 * time.Second
	d := min(base<<min(attempt, 20), ceiling)      // min(attempt,20) กัน shift overflow
	return time.Duration(rand.Int64N(int64(d) + 1)) // full jitter: สุ่มใน [0, d]
}
```

```
attempt:      1        2          3            4              5
เพดาน:      200ms    400ms      800ms         1.6s           3.2s
ค่าจริง:  [0,200ms] [0,400ms] [0,800ms]   [0,1600ms]     [0,3200ms]  ← สุ่มทั้งช่วง
```

#### รายละเอียดที่ต้องระวังในโค้ด 3 บรรทัดนี้

| จุด | ทำไม |
|---|---|
| `min(attempt, 20)` | `time.Duration` เป็น `int64`. `100ms = 1e8 ns`; `1e8 << 20 ≈ 1.05e14` ปลอดภัย. ถ้า `attempt = 64` การ shift จะ **overflow เป็นค่าติดลบ** → `rand.Int64N` panic |
| `min(..., ceiling)` | เพดานสัมบูรณ์ — งานที่ retry ครั้งที่ 20 ไม่ควรรอ 3 ปี |
| `int64(d) + 1` | `rand.Int64N(n)` คืนค่าใน `[0, n)` — **ไม่รวม `n`** ⇒ `+1` เพื่อให้รวมค่าสูงสุด |
| `math/rand/v2` | goroutine-safe, ไม่ต้อง seed, **ไม่มี global mutex แบบ v1** (v1 `rand.Int63n` ใช้ล็อกร่วม → contention ตอน worker เยอะ) |

#### ทำไมต้อง jitter — thundering herd

```
DB ล่ม → 5,000 งานล้มพร้อมกันที่ t=0

ไม่มี jitter (backoff = 2^n วินาที):
  t=2s    ████████████████████ 5,000 งานชนพร้อมกัน → DB ล้มซ้ำ
  t=6s    ████████████████████ 5,000 งานชนพร้อมกัน → DB ล้มซ้ำ
  t=14s   ████████████████████ ...ไม่มีวันฟื้น

full jitter:
  t=0-2s  ▂▃▂▃▂▃▂▃▂▃▂▃▂▃▂▃▂▃ กระจายสม่ำเสมอ → DB ค่อย ๆ รับได้
  t=2-6s  ▂▃▂▃▂▃▂▃▂▃▂▃        → ฟื้นตัว
```

#### เปรียบเทียบ 4 สูตร

สูตรทั้งสี่ยกมาจากบทความ AWS ตรง ๆ:

| สูตร | นิยาม | ผลตามที่ AWS รายงาน |
|---|---|---|
| no jitter | `min(cap, base·2ⁿ)` | **"the clear loser"** — ชนกันหมด อย่าใช้ |
| equal jitter | `d/2 + rand(0, d/2)` | จำนวนครั้งที่เรียก **ใกล้เคียง** full jitter แต่ใช้เวลารวมนานกว่ามาก |
| **full jitter** ✅ | `rand(0, min(cap, base·2ⁿ))` | ดีทั้งจำนวนครั้งและเวลารวม |
| decorrelated | `min(cap, rand(base, prev·3))` | เวลารวมสั้น แต่**จำนวนครั้งที่เรียกสูงกว่า** และต้องจำ `prev` (มี state) |

> ⚠️ บทความ AWS แสดงผลเป็น**กราฟเปรียบเทียบ ไม่ใช่ตารางตัวเลข** และสรุปว่า
> full jitter กับ decorrelated jitter **เป็นตัวเลือกที่ดีทั้งคู่** — ไม่ได้ประกาศผู้ชนะเดียว
> (เอกสารฉบับก่อนของเราเขียนว่า full jitter "เรียกน้อยที่สุด" ซึ่ง**เกินกว่าที่แหล่งอ้างอิงรองรับ**)

**เหตุผลที่เราเลือก full jitter คือมันไม่ต้องจำ state** — `Backoff(attempt)` เป็น pure function
คำนวณจาก `Attempt` ที่คิวเก็บให้อยู่แล้ว ⇒ ทำงานถูกต้องแม้ worker คนละตัวรับงานต่อจากคนที่ตายไป
ส่วน decorrelated ต้องพก `prev` ไปกับงาน ซึ่งหายไปพร้อม worker ที่ตาย

> ผลข้างเคียงที่ **ตั้งใจ**: full jitter คืนค่าใกล้ 0 ได้ ⇒ retry ทันที
> ถ้ารับไม่ได้ (เช่น ต้องเว้นอย่างน้อย 1 วิเสมอ) ให้ใช้ `base + rand(0, d)`

### 4.6 Worker pool + panic guard + graceful shutdown

```go
func RunPool(ctx context.Context, q *MemQueue, workers int, timeout time.Duration, h Handler) {
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)                        // ★ Add ก่อน go เสมอ ไม่งั้น Wait อาจผ่านก่อนเริ่ม
		go func() {
			defer wg.Done()
			for {
				j, err := q.Dequeue(ctx)
				if err != nil {
					return               // ctx.Err() หรือ ErrClosed → ออกอย่างสงบ
				}
				if err := safeCall(ctx, timeout, h, j); err != nil {
					q.Nack(j.ID, Backoff(j.Attempt), err) // err → Job.LastErr
					continue
				}
				q.Ack(j.ID)
			}
		}()
	}
	wg.Wait()
}

func safeCall(ctx context.Context, timeout time.Duration, h Handler, j *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)   // งานหนึ่งชิ้น panic ห้ามล้มทั้งโปรเซส
		}
	}()
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()                             // ไม่ cancel = ctx leak สะสม
	return h(cctx, j)
}
```

#### รายละเอียดที่สำคัญ

**`wg.Add(1)` ต้องอยู่ก่อน `go`** — ถ้าเขียน `go func(){ wg.Add(1); ... }()`
จะมีช่วงเวลาที่ `wg.Wait()` เห็น counter = 0 แล้วผ่านไปเลย ทั้งที่ goroutine ยังไม่เริ่ม

**`recover` ทำงานเฉพาะใน goroutine ของตัวเอง** — ถ้า handler ไปเรียก
`go func(){ ... }()` แล้ว panic ข้างใน goroutine นั้น `safeCall` ช่วยไม่ได้ **โปรเซสตายทั้งใบ**
(นี่คือดีไซน์ตั้งใจของ Go: panic ที่ไม่มีคนรับ = โปรแกรมอยู่ในสถานะที่ไว้ใจไม่ได้)

**`err` ต้องเป็น named return** — `recover()` ใน `defer` แก้ค่าที่จะคืนได้ก็ต่อเมื่อ
return value มีชื่อ. เขียน `func(...) error` เฉย ๆ แล้ว `defer` กำหนดค่าไม่ได้

**ทำไมไม่ใช้ `errgroup`** — `golang.org/x/sync/errgroup` มี `SetLimit(n)` ที่ทำ pool ได้
แต่มันออกแบบมาเพื่อ "งานชุดหนึ่งที่จบแล้วรวมผล" ไม่ใช่ worker ที่วนตลอดกาล
และมันจะ **ยกเลิกทุกอย่างเมื่อมีตัวใดล้ม** ซึ่งตรงข้ามกับที่ต้องการ (งานล้ม 1 ชิ้นต้องไม่กระทบตัวอื่น)
⇒ `sync.WaitGroup` เปล่า ๆ ถูกต้องกว่าและเป็น stdlib

#### Graceful shutdown 2 จังหวะ

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

pool := make(chan struct{})
go func() { defer close(pool); RunPool(ctx, q, 8, 30*time.Second, handle) }()

<-ctx.Done()                          // จังหวะ 1: SIGTERM → หยุดรับงานใหม่
                                      //   งานที่ inflight ทำต่อจนจบ (ctx ที่ handler ได้
                                      //   ถูก cancel ด้วย → handler ที่เคารพ ctx จะรีบจบ)
select {
case <-pool:                          // จังหวะ 2: รอ drain
	log.Info("drain สำเร็จ")
case <-time.After(25 * time.Second):  //   เกินเวลา → ปล่อยตาย
	log.Warn("drain timeout — งานค้างจะถูกส่งซ้ำเองหลัง lease หมด")
}
```

**ลำดับ timeout ที่ต้องเรียงให้ถูก:**

```
handler timeout (30s?)  <  drain timeout (25s)  <  terminationGracePeriodSeconds (30s)
        ▲                          ▲                            ▲
   ต่อ 1 งาน               รอ worker ออกครบ            K8s ส่ง SIGKILL
```

⚠️ ในตัวอย่างข้างบน handler timeout (30s) > drain timeout (25s) — **ตั้งใจ**:
งานที่ยาวกว่า 25 วิจะถูกฆ่ากลางคัน แต่ **กลับมาเองหลัง lease หมด**
ถ้าอยากให้ drain สำเร็จเสมอ ต้องตั้ง handler timeout < drain timeout

> **นี่คือผลตอบแทนของ at-least-once:** deploy กลางวันได้โดยไม่ต้องรองานยาวที่สุดจบ
> เพราะงานที่ถูกฆ่าไม่หาย

---

## 5. Production: Postgres + `SKIP LOCKED`

คำตอบจริงสำหรับงานที่ห้ามหาย. `SKIP LOCKED` (PG 9.5+) ทำให้ "คิวใน DB" เลิกเป็น anti-pattern

### 5.1 Schema

```sql
CREATE TABLE jobs (
  id          bigserial   PRIMARY KEY,
  queue       text        NOT NULL,
  payload     jsonb       NOT NULL,     -- ต้องมี "v": 1 เสมอ (ดู §5.4)
  priority    smallint    NOT NULL DEFAULT 0,
  state       text        NOT NULL DEFAULT 'ready',   -- ready | running | dead
  run_at      timestamptz NOT NULL DEFAULT now(),     -- = Job.RunAt
  lease_until timestamptz,                            -- = Job.leaseUntil
  lease_token uuid,                                   -- ★ fencing token (ดู §5.4)
  attempt     int         NOT NULL DEFAULT 0,         -- = Job.Attempt (I3)
  max_attempt int         NOT NULL DEFAULT 5,
  last_error  text,
  created_at  timestamptz NOT NULL DEFAULT now()      -- = Job.enqueued (I4)
);

-- partial index: แถวที่ทำเสร็จแล้วไม่กินพื้นที่ index เลย
CREATE INDEX jobs_pick  ON jobs (queue, priority DESC, run_at) WHERE state = 'ready';
CREATE INDEX jobs_lease ON jobs (lease_until)                  WHERE state = 'running';
```

**ทำไม partial index สำคัญมาก:** ในคิวที่ทำงานปกติ 99.9% ของแถวจะเป็น `done` (ถูกลบ)
หรือ `running` — ถ้า index ทั้งตาราง มันจะโตตามปริมาณงานสะสม ทำให้ `Dequeue` ช้าลงเรื่อย ๆ
partial index เก็บแค่แถวที่ `state='ready'` ⇒ **ขนาดคงที่ตามความลึกคิว ไม่ใช่ตามประวัติ**

**ทำไมลำดับคอลัมน์เป็น `(queue, priority DESC, run_at)`:** ตรงกับ `ORDER BY` ใน query เป๊ะ
⇒ Postgres อ่าน index เรียงมาได้เลย ไม่ต้อง sort — เทียบเท่า comparator ของ `ready` heap ([§4.1](#41-priority--fifo--binary-heap-ที่มี-tie-break))

### 5.2 Dequeue — atomic, worker กี่ตัวก็ไม่ชนกัน

```sql
UPDATE jobs SET state       = 'running',
                attempt     = attempt + 1,          -- I3: นับตอน dequeue
                lease_until = now() + $3::interval, -- visibility timeout
                lease_token = gen_random_uuid()     -- ★ ตั๋วประจำ lease รอบนี้ (§5.4)
WHERE id IN (
    SELECT id FROM jobs
    WHERE queue = $1 AND state = 'ready' AND run_at <= now()
    ORDER BY priority DESC, run_at                  -- = comparator ของ ready heap
    FOR UPDATE SKIP LOCKED                          -- ★ หัวใจ
    LIMIT $2
)
RETURNING id, payload, attempt, max_attempt, lease_token;
```

#### `SKIP LOCKED` ทำอะไรจริง ๆ

`FOR UPDATE` = จองแถว (row-level exclusive lock) จนจบ transaction

| ไม่มี `SKIP LOCKED` | มี `SKIP LOCKED` |
|---|---|
| worker B เจอแถวที่ A จองอยู่ → **รอ** | worker B **ข้ามไปแถวถัดไปทันที** |
| ทุก worker เข้าคิวรอแถวเดียวกัน (แถวแรกตาม `ORDER BY`) | แต่ละ worker ได้คนละแถว |
| throughput = 1 worker | throughput = N worker |

```
ไม่มี SKIP LOCKED:              มี SKIP LOCKED:
  A ─┐                            A ──▶ row 1
  B ─┼──▶ row 1 (คิวรอ)           B ──▶ row 2  (ข้าม row 1)
  C ─┘                            C ──▶ row 3  (ข้าม row 1,2)
```

**ทำไม `IN (SELECT ... FOR UPDATE SKIP LOCKED)` ปลอดภัย:** subquery ล็อกแถวที่เลือกไว้แล้ว
ก่อนที่ `UPDATE` ชั้นนอกจะทำงาน ⇒ ไม่มี worker อื่นแทรกได้ระหว่างนั้น
(เขียน `UPDATE ... WHERE state='ready' ... LIMIT` ตรง ๆ ไม่ได้ เพราะ `UPDATE` ไม่รับ `LIMIT`
และไม่มี `SKIP LOCKED`)

#### สองคำเตือนจากเอกสาร PostgreSQL เองที่ต้องรู้

> **1. `SKIP LOCKED` ให้มุมมองข้อมูลที่ไม่สอดคล้อง (inconsistent view)**
> เอกสาร PG ระบุตรง ๆ ว่า *"...จึงไม่เหมาะกับงานทั่วไป แต่ใช้เพื่อเลี่ยง lock contention ได้
> เมื่อมีผู้บริโภคหลายรายเข้าถึงตารางที่ทำหน้าที่เป็นคิว"*
> — แปลว่า **อย่าใช้รูปแบบนี้กับ query ที่ต้องการคำตอบครบถ้วน** (เช่น รายงาน, การนับยอด)
> มันถูกออกแบบมาเพื่อคิวโดยเฉพาะ ซึ่งคือกรณีของเราพอดี

> **2. locking clause ใน sub-SELECT ล็อกเฉพาะแถวที่ถูกส่งคืนให้ query ชั้นนอก**
> ตัวอย่างจากเอกสาร: `SELECT * FROM (SELECT * FROM t FOR UPDATE) ss WHERE col1 = 5;`
> จะล็อกเฉพาะแถวที่ `col1 = 5` ทั้งที่เงื่อนไขนั้นไม่ได้อยู่ใน sub-query
> — **ในรูปแบบของเราปลอดภัย** เพราะ `WHERE id IN (...)` ใช้ id ทุกตัวที่ subquery คืนมา
> แต่ถ้าไปเติมเงื่อนไขอื่นใน `UPDATE ... WHERE` เมื่อไหร่ แถวที่ถูกกรองออกจะ**ไม่ถูกล็อก**
> ⇒ **ห้ามใส่เงื่อนไขเพิ่มใน WHERE ชั้นนอก** ใส่ใน subquery เท่านั้น

`FOR UPDATE SKIP LOCKED` มีผลกับ **row-level lock เท่านั้น** — table-level `ROW SHARE`
ยังถูกจับตามปกติ (ไม่กระทบการใช้งานคิว แต่ต้องรู้ถ้ามี DDL วิ่งพร้อมกัน)

#### `LIMIT $2` — batch dequeue มีทั้งได้และเสีย

| batch ใหญ่ | batch เล็ก (1) |
|---|---|
| ✅ round-trip น้อย → throughput สูง | ❌ round-trip ทุกงาน |
| ❌ worker ถือ lease หลายงานพร้อมกัน — ตายทีเดียวเสียหลายงาน | ✅ เสียทีละงาน |
| ❌ งานกระจายไม่สม่ำเสมอ (คนแรกกวาดไป 100) | ✅ กระจายดี |

**เริ่มที่ `LIMIT 1`** แล้วเพิ่มเมื่อวัดได้ว่า round-trip เป็นคอขวดจริง

### 5.3 เหตุผลจริงที่เลือก Postgres — Transactional Outbox

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil { return err }
defer tx.Rollback()

tx.ExecContext(ctx, `INSERT INTO orders (...) VALUES (...)`)                  // business data
tx.ExecContext(ctx, `INSERT INTO jobs (queue, payload) VALUES ('email', $1)`, p) // งาน

return tx.Commit() // ★ order กับ job commit พร้อมกันหรือไม่เกิดเลย — ไม่มีสถานะกลาง
```

**ปัญหาที่มันแก้:**

```
❌ Redis/Kafka:
   1. INSERT order      → COMMIT สำเร็จ
   2. publish "email"   → 💥 network ล่ม
   ⇒ order นั้นไม่มีอีเมลตลอดกาล ไม่มีใครรู้ ไม่มี retry
   ⇒ ค้นพบตอนลูกค้าโทรมาถาม 3 วันให้หลัง

✅ Postgres:
   1. INSERT order + INSERT job → COMMIT (atomic)
   ⇒ ล้มก็ล้มทั้งคู่ ⇒ ไม่มีสถานะที่ order มีแต่งานไม่มี
```

บั๊กแบบนี้ **หายาก แก้ยาก และมักโผล่ตอน incident** เพราะเกิดตอนระบบไม่ปกติเท่านั้น
Postgres ปิดช่องนี้ **ฟรี** — นี่คือเหตุผลอันดับหนึ่งที่ควรเลือกมัน

### 5.4 สิ่งที่ต้องทำ ไม่งั้นเจ็บ

| # | ปัญหา | อาการ | วิธีแก้ |
|---|---|---|---|
| 1 | **ตารางบวม (bloat)** — คิวคือ UPDATE/DELETE หนัก ทุก UPDATE สร้าง tuple ใหม่ทิ้งตัวเก่าเป็นขยะ | `Dequeue` ค่อย ๆ ช้าลงเป็นสัปดาห์ | `ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);` |
| 2 | **transaction ค้าง (`idle in transaction`)** | vacuum เก็บขยะไม่ได้ **ทั้งฐานข้อมูล** เพราะ xmin horizon ค้าง | ตั้ง `idle_in_transaction_session_timeout = '30s'`; ห้ามเปิด tx คร่อมการเรียก external API |
| 3 | **polling เปลืองเปล่า** | CPU ของ DB เต็มไปด้วย query ที่ไม่เจออะไร | `LISTEN`/`NOTIFY` + `pgx` `WaitForNotification` — **ต้องมี ticker สำรอง** (เช่น ทุก 5 วิ) เพราะ NOTIFY ไม่ durable: ไม่มี listener ตอนนั้น สัญญาณหายเลย (payload จำกัด 8,000 ไบต์ด้วย) |
| 4 | **payload ใหญ่** — แถวเกิน ~**2 KB** จะถูก TOAST (บีบอัด/ย้ายออกนอก page) | อ่านช้าลงชัดเจน, WAL บวม | **claim check**: เก็บของจริงใน S3 ใส่แค่ key ในคิว |
| 5 | **deploy แล้วงานเก่าค้างในคิว** | consumer ใหม่ unmarshal payload เก่าไม่ได้ → poison pill ทั้งกอง | `payload` ต้องมี `"v": 1` เสมอ; consumer ใหม่ต้องอ่านของเก่าออกได้อย่างน้อย 1 เวอร์ชัน |
| 6 | **connection pool หมด** | worker block รอ connection = worker ที่ตายแล้ว | `SetMaxOpenConns` ≥ จำนวน worker + margin; อย่าลืมว่า handler ก็ใช้ connection |
| 7 | **DELETE vs UPDATE state='done'** | ตารางโตไม่หยุด, index บวม | **`DELETE` ทันทีที่ Ack**; ถ้าต้องเก็บประวัติให้ `INSERT INTO jobs_archive` ใน tx เดียวกัน |

#### Reaper (จำเป็นใน Postgres ต่างจาก in-memory)

```sql
-- รันทุก 30 วิ จาก instance ไหนก็ได้ — idempotent จึงรันซ้อนกันได้ไม่มีปัญหา
-- ล้าง lease_token ด้วย: ตั๋วเก่าต้องใช้ไม่ได้ทันทีที่ lease หมด
UPDATE jobs SET state = 'ready', run_at = now(), lease_token = NULL
WHERE state = 'running' AND lease_until < now();
```

#### Ack / Nack — ต้องพก `lease_token` เสมอ

```sql
-- Ack: DELETE ดีกว่า UPDATE state='done' — กันตารางบวม (ดูข้อ 7)
DELETE FROM jobs WHERE id = $1 AND lease_token = $2;

-- Nack: ตรรกะเดียวกับ retryLocked() ทุกประการ
UPDATE jobs SET state       = CASE WHEN attempt >= max_attempt THEN 'dead' ELSE 'ready' END,
                run_at      = now() + $2::interval,
                last_error  = $3,
                lease_token = NULL
WHERE id = $1 AND lease_token = $4;
```

**ทั้งสอง query ต้องเช็ก `rows affected`** — ได้ 0 แถวแปลว่าเสีย lease ไปแล้ว
เทียบเท่า `ErrNotInflight` ของ `MemQueue` ⇒ ต้องนับเป็น `AckTooLate` และ**หยุดทำงานทันที**

#### ทำไม `WHERE id = $1` เฉย ๆ ไม่พอ — fencing token

`MemQueue` เช็ก `q.inflight[id]` ก่อนทุกครั้ง ⇒ worker ที่เสีย lease ไปแล้ว `Ack`/`Nack` ไม่ได้
ถ้าแปลเป็น SQL แบบตรงตัวว่า `DELETE FROM jobs WHERE id = $1` **การป้องกันนั้นหายไปทั้งหมด**:

```
t0  worker A: dequeue job 42 (lease 30 วิ) → GC pause / network ค้าง 35 วิ
t1  reaper: lease หมด → job 42 กลับเป็น 'ready'
t2  worker B: dequeue job 42 → เริ่มทำใหม่
t3  worker A: ฟื้น → DELETE FROM jobs WHERE id = 42
    ⇒ ลบงานที่ worker B **กำลังทำอยู่**; ถ้า B ล้มทีหลัง ไม่มีอะไรให้ retry แล้ว
    ⇒ at-least-once กลายเป็น at-most-once เงียบ ๆ  ← งานหาย
```

`Nack` แย่กว่า: worker A ที่เสีย lease ไปแล้ว reset งานของ B กลับเป็น `'ready'` ทั้งที่ B ยังทำอยู่
⇒ งานถูกทำพร้อมกันสองที่โดยที่ระบบคิดว่ามีตัวเดียว

`lease_token` คือ **fencing token** ตัวเดียวกับที่ SQS เรียก *receipt handle*:
เกิดใหม่ทุกครั้งที่ dequeue ⇒ ตั๋วของ A ใช้ไม่ได้แล้วตั้งแต่ reaper ล้างมันทิ้ง
(เช็ก `lease_until > now()` แทนไม่พอ เพราะ B ต่ออายุ lease ไปแล้ว เงื่อนไขจึงเป็นจริงอีกครั้ง)

### 5.5 เมื่อไหร่ Postgres ไม่พอ

| อาการ | เพดานคร่าว ๆ | ไปทางไหนต่อ |
|---|---|---|
| `Dequeue` แย่ง lock กันเอง | > ~2,000 job/s | partition ตาราง `jobs` ตาม `queue` หรือ hash |
| WAL เขียนไม่ทัน | payload ใหญ่ × throughput สูง | claim check (ข้อ 4) ก่อน แล้วค่อยย้ายไป Redis/Kafka |
| ต้อง fan-out ให้หลายทีม | — | Kafka — คนละเครื่องมือ ([§1.1](#11-queue-vs-streamlog--ต่างกันที่-ข้อความหายไปไหนหลังอ่าน)) |
| ต้อง replay ย้อนหลัง | — | Kafka |

---

## 6. Operations: metric, sizing, failure mode

### 6.1 Metric ที่ต้อง alert คือ "อายุงานที่เก่าที่สุด" ไม่ใช่ "ความลึกคิว"

```
depth = 10,000 ที่ 10,000 job/s → รอ 1 วินาที       ปกติ
depth =    100 ที่      1 job/s → รอ 100 วินาที     🔥 ไฟไหม้
```

**depth เพียงอย่างเดียวไม่มีความหมาย** เพราะไม่รู้อัตราการระบาย
`oldest_ready_seconds` รวมข้อมูลทั้งสองไว้แล้วในตัวเลขเดียว และเป็นสิ่งที่ผู้ใช้รู้สึกได้จริง

```
queue_oldest_ready_seconds   gauge      ← ★ ตั้ง alert ที่นี่ (SLI ตัวเดียวก็พอ)
queue_depth{state}           gauge      ← ไว้ debug: ready/delayed/inflight/dead
job_duration_seconds         histogram  ← หา handler ที่ช้า (p50/p95/p99)
job_total{outcome}           counter    ← success | retry | dead
queue_ack_too_late_total     counter    ← ★ Stats.AckTooLate — VT สั้นไป (§6.4)
queue_dead_dropped_total     counter    ← ★ Stats.DeadDropped — DLQ ล้น ไม่มีใครระบาย
```

#### ตั้ง threshold อย่างไร

```
warn  : oldest_ready_seconds > SLO ของงานนั้น × 0.5
page  : oldest_ready_seconds > SLO ของงานนั้น
page  : increase(job_total{outcome="dead"}[5m]) > 0        ← DLQ เพิ่มแม้แต่ 1 ก็ page
warn  : rate(queue_ack_too_late_total[5m]) > 0            ← visibility timeout สั้นไป
page  : increase(queue_dead_dropped_total[5m]) > 0        ← DLQ ล้นแล้ว = หลักฐานหาย
```

**DLQ เพิ่มขึ้นแม้แต่ 1 = page ทันที** — งานที่ตกที่นั่นแปลว่าลองมาแล้ว N ครั้งไม่ผ่าน
มันจะไม่หายไปเอง และไม่มีใครรู้ถ้าไม่แจ้ง

#### เชื่อม Stats เข้า Prometheus

```go
// ponytail: ไม่ต้องมี exporter แยก — collector เดียวอ่านจาก Stats()
prometheus.MustRegister(prometheus.NewGaugeFunc(
	prometheus.GaugeOpts{Name: "queue_oldest_ready_seconds"},
	func() float64 { return q.Stats().OldestReady.Seconds() },
))
prometheus.MustRegister(prometheus.NewCounterFunc(
	prometheus.CounterOpts{Name: "queue_ack_too_late_total"},
	func() float64 { return float64(q.Stats().AckTooLate) }, // ★ VT สั้นเกินไป
))
```

> `Stats.AckTooLate` เพิ่มเข้ามาตอนรีวิว — ก่อนหน้านี้เอกสารสั่งให้ตั้ง alert
> ที่ `ack_too_late_total` แต่ `RunPool` กลืน error จาก `Ack`/`Nack` ทิ้ง
> ⇒ **metric ที่แนะนำนั้นเก็บไม่ได้จริง** ตอนนี้คิวนับให้เองแล้ว

### 6.2 หาจำนวน worker ด้วย Little's Law อย่าเดา

> **L = λ × W** — จำนวนงานที่อยู่ในระบบ = อัตรางานเข้า × เวลาที่งานอยู่ในระบบ

กฎนี้เป็นจริง**ไม่ว่าการกระจายตัวจะเป็นแบบไหน** ขอแค่ระบบเสถียร (ไม่โตไม่จำกัด)

#### ขั้นที่ 1: หาจำนวน worker ขั้นต่ำ

λ = 500 job/s, งานละ 50 ms ⇒ ต้องมี `500 × 0.05 = 25` worker แค่จะ *ตามทัน*

#### ขั้นที่ 2: เผื่อ utilization

`ρ = λ / (c·μ)` โดย `c` = จำนวน worker, `μ = 1/50ms = 20 งาน/วินาที/worker`

สำหรับ M/M/1 เวลารอในคิว `Wq = ρ / (μ(1-ρ))` ⇒ **`Wq ∝ ρ/(1-ρ)`**:

| ρ | `ρ/(1-ρ)` | เทียบกับ ρ=0.5 |
|---|---|---|
| 0.50 | 1.0 | 1× |
| 0.70 | 2.3 | 2.3× |
| 0.90 | 9.0 | 9× |
| 0.95 | 19.0 | **19×** |
| 0.99 | 99.0 | **99×** |

```
เวลารอ
  ▲                                          ╱
  │                                        ╱
  │                                    ╱
  │                            ╱╱╱╱
  │              ╱╱╱╱╱╱╱
  └──────────────────────────────────────────▶  ρ
  0        0.5      0.7      0.9   0.95   1.0
                     ↑              ↑
                  ปลอดภัย      รอนานกว่า 19 เท่า
```

นี่คือเหตุผลที่ระบบ "ดูโหลด 90% ก็ยังไหว" **แล้วพังทันทีที่ traffic ขึ้นอีก 5%**
— เส้นโค้งไม่เป็นเชิงเส้น ความรู้สึกจากกราฟ CPU หลอกได้

⇒ ตั้งเป้า **ρ ≤ 0.7** ⇒ **c = 25 ÷ 0.7 ≈ 36 worker**

#### ขั้นที่ 3: หา capacity ของคิว

ต้องการ p99 ของเวลารอ ≤ 2 วินาที ที่ λ = 500 ⇒ `L = λW = 500 × 2 = 1,000`

⇒ ตั้ง `capacity = 1000` แล้ว **reject ที่เหลือ** — ดีกว่ารับเข้ามาแล้วให้รอ 30 วินาที
เพราะ client ส่วนใหญ่ timeout ไปก่อนอยู่ดี = ทำไปก็เสียเปล่า

#### ขั้นที่ 4: หา visibility timeout

```
VT = p99.9 ของ job_duration_seconds × 2   (เผื่อ GC pause, network hiccup, CPU steal)
```

ต่ำไป → งานถูกทำซ้ำโดยไม่จำเป็น ([§6.4](#64-งานที่ยาวกว่า-visibility-timeout-จะถูกทำซ้ำตลอดกาล))
สูงไป → worker ตายแล้วงานค้างนาน (แต่ไม่หาย) — **ผิดพลาดไปทางสูงปลอดภัยกว่า**

### 6.3 Idempotency ไม่ใช่ nice-to-have

at-least-once = งานจะซ้ำ **แน่นอน** — worker ตาย, network timeout ตอน ack,
lease หมดขณะยังทำอยู่ ([§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ))

#### ท่ามาตรฐาน: dedup table ใน transaction เดียวกับ side effect

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil { return err }
defer tx.Rollback()

res, err := tx.ExecContext(ctx,
	`INSERT INTO processed_jobs (job_id) VALUES ($1) ON CONFLICT DO NOTHING`, j.ID)
if err != nil { return err }
if n, _ := res.RowsAffected(); n == 0 {
	return nil                      // เคยทำแล้ว → ack เฉย ๆ ไม่ทำซ้ำ
}

chargeCard(ctx, tx, ...)            // ★ side effect ต้องอยู่ tx เดียวกับ dedup
return tx.Commit()                  //   ไม่งั้น dedup ไม่มีความหมาย
```

**ทำไมต้องอยู่ tx เดียวกัน:** ถ้า dedup commit แยกจาก side effect
จะเกิดกรณี "บันทึกว่าทำแล้ว แต่ยังไม่ได้ทำ" ⇒ กลายเป็น at-most-once ⇒ **งานหาย**
— เป็นข้อผิดพลาดเดียวกับ [§1.2](#12-delivery-semantics--เรื่องของ-ack-เมื่อไหร่-ล้วน-ๆ) แค่ย้ายที่

**อย่าลืมเก็บกวาด** `processed_jobs`:
```sql
DELETE FROM processed_jobs WHERE created_at < now() - interval '7 days';
```
(7 วัน > อายุสูงสุดที่งานจะยังวน retry อยู่ได้)

#### 4 ระดับของ idempotency — เลือกตามสิ่งที่ทำ

| ระดับ | วิธี | ใช้เมื่อ |
|---|---|---|
| **โดยธรรมชาติ** | operation ที่ทำซ้ำแล้วผลเหมือนเดิม: `SET status='paid'`, `PUT /resource/42` | ดีที่สุด — ออกแบบให้เป็นแบบนี้ถ้าทำได้ |
| **dedup table** | ตัวอย่างข้างบน | side effect อยู่ใน DB เดียวกัน |
| **idempotency key ของ API ปลายทาง** | ส่ง `j.ID` เป็น `Idempotency-Key` (Stripe, SendGrid รองรับ) | เรียก external API |
| **fencing token** | `WHERE fence < $attempt` ([§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ)) | ต้องกัน worker เก่าเขียนทับของใหม่ |

### 6.4 งานที่ยาวกว่า visibility timeout จะถูกทำซ้ำตลอดกาล

```
VT = 30s, งานใช้ 45s:
  t=0    worker A รับงาน  lease→30s
  t=30   lease หมด → คิวส่งให้ worker B  lease→60s     ← A ยังทำอยู่!
  t=45   worker A ทำเสร็จ Ack → ErrNotInflight (สายไปแล้ว)
  t=60   lease ของ B หมด → ส่งให้ worker C ...
  t=75   B ทำเสร็จ → ErrNotInflight
  ⇒ วนไม่จบ กิน CPU เต็ม งานสำเร็จซ้ำ ๆ Attempt พุ่งจนตกลง DLQ ทั้งที่ทำสำเร็จทุกครั้ง
```

**อาการที่เห็นใน metric:** `ack_too_late_total` พุ่ง + `job_total{outcome="dead"}` เพิ่ม
ทั้งที่ handler ไม่เคยคืน error

#### แก้สองทาง

**(ก) ตั้ง `VT > p99.9 × 2`** ← ทำได้เดี๋ยวนี้ ไม่ต้องแก้โค้ด

**(ข) heartbeat ด้วย `Extend(id, d)`** ← **มีใน repo นี้แล้ว** (เพิ่มตอนรีวิวรอบสอง)
เทียบเท่า `ChangeMessageVisibility` ของ SQS ซึ่งเอกสาร AWS เองก็แนะนำท่านี้ตรง ๆ:
*"Implement a heartbeat mechanism to periodically extend the visibility timeout"*

```go
// ฝั่ง worker: heartbeat ทุก VT/3
func withHeartbeat(ctx context.Context, q *queue.MemQueue, j *queue.Job,
	vt time.Duration, work func(context.Context) error) error {

	hctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		t := time.NewTicker(vt / 3)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := q.Extend(j.ID, vt); err != nil {
					cancel()   // ★ เสีย lease แล้ว → หยุดทำงานทันที ปล่อยให้คนใหม่ทำ
					return
				}
			case <-hctx.Done():
				return
			}
		}
	}()
	return work(hctx)
}
```

**จุดสำคัญที่คนทำผิดบ่อย:** เมื่อ `Extend` ล้มเหลว ต้อง **`cancel()` หยุดทำงานทันที**
ไม่ใช่ log แล้วทำต่อ — เพราะมี worker อื่นรับงานไปทำแล้ว การทำต่อคือการทำซ้ำ
ที่ *ป้องกันได้* (ต่างจากการทำซ้ำที่หลีกเลี่ยงไม่ได้ใน [§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ))

> ⚠️ SQS มีเพดาน **12 ชั่วโมงนับจากที่รับข้อความครั้งแรก** — `Extend` ไม่รีเซ็ตเพดานนี้
> งานที่ยาวกว่านั้นต้องซอยเป็นขั้นย่อย (AWS แนะนำ Step Functions). ถ้าออกแบบให้พึ่ง
> `Extend` ไม่จำกัด แล้ววันหนึ่งย้ายไป SQS จะเจอกำแพงนี้

**จุดสำคัญ:** เมื่อ `Extend` ล้มต้อง **หยุดทำงานทันที** ไม่ใช่ทำต่อ — เพราะมี worker
อื่นรับงานไปแล้ว การทำต่อคือการทำซ้ำที่ป้องกันได้ (SQS เรียกท่านี้ว่า `ChangeMessageVisibility`)

### 6.5 Head-of-line blocking

```
คิวเดียว, worker 8 ตัว:
  [ export CSV 10 นาที ] × 8  ← กิน worker หมด
  [ ส่ง OTP 50ms ] ← รอ 10 นาที  🔥 ผู้ใช้ล็อกอินไม่ได้
```

**แก้: แยกคิวตามคลาสของงาน + pool แยก**

```go
fast := NewMemQueue(10_000, 5, 30*time.Second)   // OTP, notification
slow := NewMemQueue(1_000, 3, 30*time.Minute)    // export, report

go RunPool(ctx, fast, 32, 5*time.Second, handleFast)
go RunPool(ctx, slow, 4, 30*time.Minute, handleSlow)
```

**ง่ายกว่า priority มาก** และไม่มีปัญหา starvation เพราะแต่ละคิวมี worker ของตัวเอง
— priority แก้ปัญหา "ใครก่อน" แต่ head-of-line blocking คือปัญหา "ทรัพยากรถูกยึด"
ซึ่ง priority แก้ไม่ได้เลย (งานด่วนที่เข้ามาทีหลังก็ยังต้องรอ worker ว่าง)

### 6.6 Priority ล้วน ๆ ทำให้งาน priority ต่ำอดตาย (starvation)

ถ้า high-priority ไหลเข้าไม่ขาด งาน `p=0` **จะไม่ได้รันเลย** เป็นชั่วโมงหรือตลอดไป

| วิธีแก้ | ทำอย่างไร | ข้อดี/เสีย |
|---|---|---|
| **aging** | `effective = Priority + age/60s` แล้ว `heap.Fix` เป็นระยะ | งานเก่าค่อย ๆ แซงขึ้นเอง / ต้องมี background pass |
| **weighted round-robin** | แยกคิว high/mid/low แล้วดึงตามอัตรา 7:2:1 | **คาดเดาได้กว่า** ทดสอบง่ายกว่า / ต้องจัดการหลายคิว |
| **จำกัดสัดส่วน high** | admission control ฝั่ง producer | แก้ที่ต้นเหตุ / ต้องคุมได้ทุก producer |

**แนะนำ weighted round-robin** — เพราะตอบคำถาม "งาน p=0 จะได้ทำภายในกี่นาที" ได้เป็นตัวเลข

### 6.7 Multi-tenant: 1 tenant ยิงล้านงานกลบทุกคน

Priority ไม่ช่วย เพราะทุกงานของ tenant นั้นก็ priority ปกติ — มันแค่ **เยอะ**

| วิธี | รายละเอียด |
|---|---|
| **per-tenant concurrency cap** | ง่ายสุด: `map[tenant]*semaphore` — tenant หนึ่งใช้ worker ได้ไม่เกิน N ตัว |
| **แยกคิวต่อ tenant + WFQ** | ยุติธรรมที่สุด แต่ต้องจัดการคิวจำนวนมาก |
| **rate limit ที่ producer** | `golang.org/x/time/rate` ต่อ tenant — แก้ที่ต้นเหตุ |

### 6.8 ทำอะไรตอน incident

| สถานการณ์ | ทำ |
|---|---|
| คิวยาวขึ้นเรื่อย ๆ | เพิ่ม worker ก่อน (ถ้า downstream รับไหว); ถ้าไม่ไหว → เปิด reject ที่ producer |
| DLQ พุ่ง | **หยุด producer ก่อน** แล้วดู `q.Dead()[0].LastErr` — มักเป็นสาเหตุเดียวกันทั้งกอง (DLQ เก็บ N ตัว*แรก* จึงเป็นตัวที่เกิดตอนปัญหาเริ่ม ไม่ใช่ตอนท่วมแล้ว) |
| งานทำซ้ำผิดปกติ | เช็ก `ack_too_late_total` → ขยาย VT ทันที ([§6.4](#64-งานที่ยาวกว่า-visibility-timeout-จะถูกทำซ้ำตลอดกาล)) |
| deploy แล้ว unmarshal พัง | rollback; งานที่ค้างจะกลับมาเองหลัง lease หมด ([§5.4](#54-สิ่งที่ต้องทำ-ไม่งั้นเจ็บ) ข้อ 5) |
| ต้อง replay DLQ | `UPDATE jobs SET state='ready', attempt=0, run_at=now() WHERE state='dead' AND id = ANY($1)` — **ทีละชุด อย่ายิงหมดทีเดียว** |

---

## 7. รีวิว: ข้อจำกัดที่รู้ตัว

เพดานของ ~350 บรรทัดนี้ ตรงไปตรงมา:

| # | ข้อจำกัด | สถานการณ์ที่พังจริง | ยกระดับเมื่อ / อย่างไร |
|---|---|---|---|
| 1 | **mutex ตัวเดียวคุมทั้งคิว** | **วัดได้:** 8 worker = ~880k job/s, 64 worker = ~320k, 512 worker = ~88k ([§3.5](#35-ตัวเลขที่วัดได้จริง-apple-m4-10-cores--benchmem)) | โปรไฟล์ (`go tool pprof -mutex`) เจอ contention → shard เป็น N คิวย่อยตาม `hash(ID) % N`, `Dequeue` วนถาม round-robin |
| 2 | **in-memory ล้วน** | deploy = งานที่ค้างหายหมด รวมถึงงานใน `delayed` และ DLQ | งานห้ามหาย → [§5](#5-production-postgres--skip-locked) |
| 3 | `Stats()` สแกน `O(n)` | เรียกใน hot loop ที่ n = 100k → หยุดคิวทุกครั้งที่เรียก | เรียกทุก 10 วิเท่านั้น; ถ้าจำเป็นเพิ่ม heap ที่ 4 เรียงตาม `enqueued` |
| 4 | `Close()` ทิ้งงานใน `delayed` | งาน retry ที่รออยู่หายตอน shutdown | ยอมรับได้ — in-memory หายอยู่แล้ว. ถ้าไม่ยอมรับ = ต้องการ durability = ข้อ 2 |
| 5 | DLQ เก็บได้แค่ `capacity` ตัว | เกินนั้นงานที่ล้มจะไม่ถูกเก็บหลักฐาน (นับใน `Stats.DeadDropped`) | ต่อ DLQ ออกที่เก็บจริง (ตาราง/S3) + alert. **เดิมข้อนี้คือ "โตไม่จำกัด" ซึ่งเป็น memory leak — แก้แล้ว** |
| 6 | **broadcast ปลุก waiter ทุกตัวเพื่องาน 1 ชิ้น** | **วัดได้:** throughput ตก ~10 เท่าจาก 8 → 512 worker. (allocation แก้ไปแล้วด้วยการใช้ timer ซ้ำ: 31 → 5 allocs/op) | worker > ~64 → waiter queue แบบ FIFO (ให้แต่ละ waiter มี channel ของตัวเอง แล้วปลุกทีละตัว) |
| 7 | ไม่มี per-key ordering / fairness | tenant เดียวกลบทุกคน; งานของ user เดียวกันสลับลำดับ | ต้องการ [§6.5](#65-head-of-line-blocking)–[§6.7](#67-multi-tenant-1-tenant-ยิงล้านงานกลบทุกคน) → แยกคิว |
| 8 | `Job.Payload` เป็น `[]byte` ไม่มี schema | deploy แล้วอ่านของเก่าไม่ออก | ใส่ version ใน payload ตั้งแต่วันแรก ([§5.4](#54-สิ่งที่ต้องทำ-ไม่งั้นเจ็บ) ข้อ 5) |
| 8b | `Dequeue` คืน**สำเนา** ⇒ +1 alloc/งาน และ `Payload []byte` ยัง share backing array | payload ใหญ่มาก ๆ ที่ handler ไปเขียนทับเอง | handler ห้ามเขียน `Payload` (อ่านอย่างเดียว); ถ้าต้องแก้ให้ copy เอง ([§3.6](#36-ขอบเขตความเป็นเจ้าของ-ทำไม-dequeue-ต้องคืนสำเนา)) |
| 9 | **`Priority` เป็นฟีเจอร์ที่คุณน่าจะไม่ต้องการ** | [§6.5](#65-head-of-line-blocking) เองบอกว่าแยกคิวดีกว่า และ [§6.6](#66-priority-ล้วน-ๆ-ทำให้งาน-priority-ต่ำอดตาย-starvation) บอกว่า priority ล้วน ๆ ทำให้เกิด starvation | **ตั้ง `Priority` เป็น 0 ทั้งหมดแล้วแยกคิวแทน** เว้นแต่พิสูจน์ได้ว่าต้องการจริง — นี่คือจุดที่ repo นี้ over-engineer ตัวเอง |

### สิ่งที่ถูกแล้ว — อย่าไปแก้

| สิ่งที่ทำ | ทำไมสำคัญ |
|---|---|
| `Attempt++` ที่ `Dequeue` ไม่ใช่ `Nack` | worker ที่ตายถูกนับด้วย → poison pill ถึง DLQ ได้ ([I3](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)) |
| capture `wake` ใต้ล็อก | ไม่มี missed wakeup ([§4.3](#43-condition-variable-ที่-select-ได้--อัลกอริทึมสำคัญที่สุดในไฟล์)) |
| full jitter ไม่ใช่ backoff คงที่ | กัน thundering herd ([§4.5](#45-exponential-backoff--full-jitter)) |
| `recover` ต่องานใน `safeCall` | งานพัง 1 ชิ้นไม่ล้มทั้ง fleet |
| **ไม่ถือ mutex ตอนเรียก handler** | ไม่งั้น concurrency กลายเป็น 1 ([I5](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)) |
| `inflight` นับรวมใน `capacity` | ไม่รับงานเกินที่ระบบทำไหว ([I6](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)) |
| `h.jobs[n-1] = nil` ใน `Pop` | ไม่ให้ backing array ถือ pointer ค้าง |
| `!After(now)` ไม่ใช่ `Before(now)` | เงื่อนไขขอบ inclusive สำหรับ deadline ([§4.4](#44-lease--visibility-timeout--ไม่ต้องมี-reaper-goroutine)) |
| ทุก method เรียก `promoteLocked()` ก่อน | พฤติกรรมไม่ขึ้นกับ traffic ที่ไม่เกี่ยวข้อง ([I2c](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)) |
| `Extend` broadcast **เฉพาะตอนย่น lease** | heartbeat เป็น path ที่ถี่ที่สุด; ปลุกทุกครั้ง = ช้าลง 14 เท่าที่ 512 waiter ([§8.3b](#83b-เพดานของ-mutation-testing--และบั๊กจริงสองตัวที่มันมองไม่เห็น)) |
| `Nack` รับ `cause error` แทนที่จะให้ worker เขียน `j.LastErr` เอง | worker ที่เสีย lease ไปแล้วอาจเขียน `*Job` ตัวเดียวกับ worker ใหม่ = data race |
| **`Dequeue`/`Dead()` คืนสำเนา ไม่ใช่ pointer ตัวจริง** | คิวเขียน `Attempt`/`leaseUntil` ต่อหลังแจกงาน — `RunPool` เองก็อ่าน `j.Attempt` นอกล็อก ([§3.6](#36-ขอบเขตความเป็นเจ้าของ-ทำไม-dequeue-ต้องคืนสำเนา)) |
| **`Dequeue` เช็ก `ctx.Err()` ก่อนดู `ready`** | ไม่งั้น SIGTERM = worker กวาด `ready` ทั้งกองมาเผา `Attempt`; deploy 5 ครั้ง × `maxAttempt=5` = งานลง DLQ ทั้งที่ไม่เคยรัน |
| `NewMemQueue` panic เมื่อ config ผิด | `visibility=0` ทำให้ทุกงานหมด lease ทันทีที่ถูกหยิบ — ต้องรู้ตอนบูต ไม่ใช่ตอนตีสาม |
| `Enqueue` รีเซ็ต `Attempt = 0` | replay จาก `Dead()` ต้องได้โควตาใหม่ ไม่ใช่เด้งกลับ DLQ ทันทีที่ล้มครั้งแรก |
| `Enqueue` ปฏิเสธ ID ว่าง | ไม่งั้นงานที่ลืมใส่ ID จองสล็อต `""` แล้วตัวถัดไปเจอ `ErrDuplicateID` ที่ไม่สื่ออะไร |

### สิ่งที่จงใจ **ไม่** สร้าง — และเงื่อนไขที่จะทำให้ควรสร้าง

รายการนี้สำคัญพอ ๆ กับรายการฟีเจอร์ เพราะทุกอย่างที่ไม่ได้เขียนคือสิ่งที่ไม่ต้องดูแล
ตอนตีสาม **อย่าสร้างล่วงหน้า — รอให้เงื่อนไขในคอลัมน์ขวาเกิดจริงก่อน**

| ไม่สร้าง | ทำไม | สร้างเมื่อ |
|---|---|---|
| **Sharding คิวเป็น N ส่วน** | วัดได้ 880k job/s ที่ 8 worker; [§0](#0-เลือกก่อนเขียนโค้ด) ชี้ให้งาน >2k job/s ไป Postgres อยู่แล้ว | `pprof -mutex` ชี้ว่า lock contention เป็นคอขวด **จริง** ไม่ใช่คาดว่าจะเป็น |
| **Waiter queue แบบ FIFO** | ได้ผลเมื่อ worker > ~64 ซึ่งตาม Little's Law คือ >1,280 job/s — ควรไป shard/Postgres แทน | รัน worker หลายร้อยตัวแล้วยังไปต่อไม่ได้ |
| **Per-key ordering** | [§1.4](#14-ordering--และเหตุผลที่-fifo-กับ-retry-ขัดกันโดยธรรมชาติ) พิสูจน์แล้วว่าขัดกับ retry โดยธรรมชาติ — แยกคิวถูกกว่าและตรวจสอบง่ายกว่า | ต้องการลำดับต่อ key **และ** ยอมรับ head-of-line blocking ในกลุ่มนั้น |
| **heap ที่ 4 ให้ `Stats` เป็น O(1)** | `Stats()` เรียกทุก 10 วินาที — เพิ่มโครงสร้างเพื่อ path ที่ไม่ร้อนคือนิยามของ over-engineering | `Stats()` โผล่ใน CPU profile |
| **ถอด `Priority` ทิ้ง** | เป็นแค่ 4 บรรทัดใน comparator — ปัญหาคือ*คำแนะนำการใช้* ซึ่งข้อ 9 จัดการแล้ว ลบ = churn เปล่า | ไม่มีใครในโค้ดเบสตั้ง `Priority ≠ 0` เลยเป็นเวลานาน |
| **บังคับ schema ให้ `Payload`** | เป็นหน้าที่ของผู้เรียก — คิวที่รู้จักเนื้อในของงานคือคิวที่ผูกกับ business logic | ไม่ควรทำในเลเยอร์นี้เลย |
| **Logger / callback / options struct** | ทุกอย่างที่ต้องสังเกตอยู่ใน `Stats()` และ `Job.LastErr` แล้ว | `Stats()` ตอบคำถามตอน incident ไม่ได้จริง ๆ |
| **Persistence ใน `MemQueue`** | นี่คือเส้นแบ่งของเครื่องมือ — [§5](#5-production-postgres--skip-locked) คือคำตอบ ไม่ใช่การเติมฟีเจอร์ที่นี่ | **ไม่มีวัน** — ย้ายไป `PgQueue` แทน |

---

## 8. การเทส — coverage, สเปก, และ "เทสจับบั๊กได้จริงไหม"

สามคำถามที่คนมักคิดว่าเป็นคำถามเดียวกัน:

| # | คำถาม | ตัววัด | เครื่องมือ | สถานะ repo นี้ |
|---|---|---|---|---|
| 1 | เทส**รันผ่าน**โค้ดบรรทัดไหนบ้าง | statement coverage | `go test -cover` | **100.0%** |
| 2 | เทสครอบ**สเปก**ครบทุกช่องไหม | ตาราง [§2.3](#23-ตารางการเปลี่ยนสถานะแบบครบถ้วน) × invariant [§2.4](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา) | เทสตาราง + `checkInvariants` | **20/20 ช่อง, 7/8 invariant** |
| 3 | เทส**จับบั๊ก**ได้จริงไหม | mutation score | `tools/mutate` | **100.0%** (141/141) |

**ข้อ 1 ไม่ได้บอกอะไรเลยเกี่ยวกับข้อ 3** และนี่คือตัวเลขจริงของ repo นี้ตอนเริ่มงาน:

```
statement coverage  99.0%   ← ดูดีมาก
mutation score      71.0%   ← 45 บั๊กที่ "เขียนได้จริง" ผ่านเทสทั้งชุดไปแบบเขียวสนิท
```

เทสรันผ่านเกือบทุกบรรทัด แต่ **ไม่ล้ม** เวลาบรรทัดนั้นทำงานผิด
coverage วัด "โค้ดถูกรัน" ไม่ได้วัด "ผลถูกตรวจ" — `assert` ที่ไม่มีวันล้มก็ให้ coverage 100% ได้

### 8.1 อย่าใช้ `time.Sleep` จริงในเทส — `testing/synctest`

**อย่าใช้ `time.Sleep` จริงในเทส** — ช้า และ flaky บน CI ที่โหลดสูง
(เทสที่ `assert(elapsed >= 80ms)` จะพังเมื่อ CI ช้า และผ่านทั้งที่โค้ดผิดเมื่อ CI เร็ว)

Go 1.25+ มี **`testing/synctest`**: นาฬิกาปลอมทั้ง bubble โดย**ไม่ต้องฉีด `Clock` interface
เข้าไปในโค้ดโปรดักชัน** — ได้ทั้งความเร็วและความสะอาดของโค้ด

```go
func TestLeaseExpiryRedelivers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const vt = 30 * time.Second
		q := NewMemQueue(100, 5, vt)
		q.Enqueue(&Job{ID: "j7"})

		a, _ := q.Dequeue(t.Context())   // worker A รับไปแล้ว "ตาย"
		_ = a

		start := time.Now()
		b, err := q.Dequeue(t.Context()) // ต้องได้งานเดิมกลับมาหลัง lease หมด
		if err != nil || b.ID != "j7" || b.Attempt != 2 {
			t.Fatalf("ส่งซ้ำ = %+v, %v", b, err)
		}
		if waited := time.Since(start); waited != vt {
			t.Errorf("ส่งซ้ำหลัง %v, ต้องการ %v พอดี", waited, vt)  // ★ เทียบเป๊ะได้
		}
	})
}
```

**ผล:** เทสที่ "รอ 30 วินาที" ใช้เวลาจริง **0.00s** และเทียบเวลาแบบ `== vt` ได้เป๊ะ
ไม่ต้องใส่ tolerance ที่ทั้งหลวมเกินและแน่นเกินพร้อมกัน

#### synctest ทำงานอย่างไร

สร้าง "bubble" ที่มีนาฬิกาของตัวเอง. **เมื่อ goroutine ทุกตัวใน bubble บล็อกอย่างถาวร**
(`durably blocked` — รอ channel, mutex, timer ที่อยู่ใน bubble ทั้งหมด)
นาฬิกาปลอมจะ **กระโดดไปที่ timer ตัวถัดไปทันที**

กฎที่ต้องรู้:

1. **goroutine ทุกตัวที่สร้างใน bubble ต้องออกก่อน `f` คืนค่า** ไม่งั้น panic
   ⇒ ต้อง `cancel()` แล้ว `<-poolDone` เสมอ
2. **ห้ามบล็อกรอสิ่งที่อยู่นอก bubble** (network จริง, channel จาก parent) — จะ panic
3. `synctest.Wait()` = รอให้ทุก goroutine ใน bubble บล็อก **โดยไม่เดินนาฬิกา**
   ใช้เมื่อต้องการ assert ตอนที่ทุกอย่างนิ่งแล้ว

#### จังหวะที่ `synctest.Wait()` เป็นตัวชี้ขาด

เทส "งานใหม่ต้องปลุก worker ที่กำลังรอ" เขียนผิดได้ง่ายมาก:

```go
go func() { j, _ := q.Dequeue(ctx); got <- j.ID }()   // waiter ยังไม่ทันบล็อก
q.Enqueue(&Job{ID: "j"})                              // ← สัญญาณปลุกออกไปก่อนมีคนรอ
```

ถ้าไม่มี `synctest.Wait()` คั่น เทสจะผ่านทั้งที่ **ลบ `broadcastLocked()` ทิ้งทั้งบรรทัด**
เพราะ waiter ไปเจองานเองตอนวนรอบแรก ไม่เคยได้ทดสอบเส้นทาง "ถูกปลุก" เลย

### 8.2 Mutation testing — วัดว่าเทส "จับบั๊กได้จริงไหม"

แนวคิด: **แก้โค้ดโปรดักชันให้ผิดทีละจุด แล้วดูว่าเทสแดงไหม**

```
killed   = เทสจับได้ ✅
survived = เทสไม่จับ ❌ ← บั๊กแบบนี้ merge เข้า main ได้โดยไม่มีใครรู้
invalid  = คอมไพล์ไม่ผ่าน (ไม่นับ — ไม่ใช่บั๊กที่เขียนได้จริง)

mutation score = killed / (killed + survived)
```

`tools/mutate` (~550 บรรทัด, `go/ast` ล้วน, ไม่มี dependency) ทำงานดังนี้:

1. parse `queue.go` แล้วเดิน AST เก็บ "จุดกลายพันธุ์" ทั้งหมด — ได้ **164 จุด**
2. worker แต่ละตัวคัดลอกแพ็กเกจไป temp dir ของตัวเอง → กลายพันธุ์พร้อมกันได้โดยไม่ชนกัน
3. `printer.Fprint` เขียนโค้ดที่กลายพันธุ์แล้ว → `go test -count=1 -failfast .`
   — mutant ถูก "ฆ่า" เมื่อมีเทสแดง ≥ 1 ตัว จึงหยุดที่ตัวแรกได้เลย (เร็วขึ้นเกือบเท่าตัว
   เพราะ 86% ของ mutant ตาย) ส่วน survivor ไม่มีเทสแดงให้หยุด = รันเต็มชุดเหมือนเดิม
   ⇒ คำตัดสินเหมือนเดิมทุกตัว
4. mutant ที่ทำให้ค้าง (เช่น ลบ `Attempt++` จนงานวนไม่จบ) นับเป็น **killed** — timeout คือการจับได้

| operator | ตัวอย่าง | จับบั๊กประเภทไหน |
|---|---|---|
| สลับตัวเปรียบเทียบ | `>` → `>=`, `<` → `>`, `==` → `!=` | off-by-one, เงื่อนไขขอบ (inclusive/exclusive) |
| สลับตรรกะ | `&&` → `\|\|` | เงื่อนไขที่ลัดวงจรผิด |
| สลับเลขคณิต | `+` → `-`, `*` → `/`, `<<` → `>>` | สูตรคำนวณ (backoff, capacity) |
| ตัด `!` | `!t.After(now)` → `t.After(now)` | ขอบเขต deadline ที่กลับด้าน |
| ลบ statement | ลบ `heap.Fix(...)`, `delete(...)`, `x++`, `a = b` | ขั้นตอนที่ลืมทำแล้ว "ดูเหมือนยังทำงาน" |
| แก้ค่าคงที่ | `0` → `1`, `1` → `0`, `n` → `n+1` | threshold ที่ hardcode |

```bash
make mutate     # → killed 141 · survived 0 · equivalent 16 · invalid 7 → 100.0%
```

#### Equivalent mutant — ปัญหาที่ตัดสินด้วยเครื่องจักรไม่ได้

mutant บางตัว "รอด" เพราะ **แก้แล้วโปรแกรมเหมือนเดิมจริง ๆ** ไม่ใช่เพราะเทสอ่อน
เช่น บรรทัดนี้:

```go
if a.Priority != b.Priority {
	return a.Priority > b.Priority   // เปลี่ยนเป็น >= ก็ให้ผลเดียวกัน — guard บนบรรทัดก่อนหน้า
}                                     // การันตีว่าสองค่าไม่เท่ากันแล้ว
return a.seq < b.seq                  // seq มาจากตัวนับที่ไม่ซ้ำ — `<` เท่ากับ `<=`
```

ตัดสินอัตโนมัติไม่ได้ (เทียบเท่าปัญหาการหยุดของทัวริง) จึงต้อง **พิสูจน์ด้วยมือทีละตัว**
แล้วบันทึกเหตุผลไว้ใน `mutation-allow.txt`:

```
# ── broadcast ใน promoteLocked: ซ้ำซ้อนกับ timer ของ waiter เอง ────────
# waiter ทุกตัวใน Dequeue ตั้ง timer ไว้ที่ min(delayed.head.RunAt,
# leases.head.leaseUntil) ตาม nextEventLocked อยู่แล้ว ...
# ⚠️ อย่าลบออกจากโค้ดจริง: ความสมมูลนี้ขึ้นกับ nextEventLocked ที่รวมสอง timeline ไว้ครบ
queue.go:MemQueue.promoteLocked:ลบ q.broadcastLocked()#0
```

**กฎ:** ห้ามเพิ่มบรรทัดในไฟล์นั้นถ้ายังเขียนเหตุผลไม่ได้ว่า "ทำไมสังเกตไม่ได้"
ไฟล์นี้คือช่องทางเดียวที่ทำให้ประตู 100% ผ่าน — ใส่มั่วเมื่อไหร่คือปิดตาตัวเองเมื่อนั้น

> **ผลพลอยได้ที่ไม่ได้ตั้งใจ:** การพิสูจน์ความสมมูลบังคับให้ต้องอ่านโค้ดในระดับที่
> รีวิวปกติไม่ไปถึง — 16 ตัวที่เหลือกลายเป็นเอกสารว่า "บรรทัดไหนคือ defence in depth
> และมันซ้ำซ้อนกับอะไร" ซึ่งเป็นความรู้ที่ปกติหายไปกับคนเขียน

### 8.3 บั๊กจริงที่ mutation หาเจอ — และเทสที่อุดมัน

45 mutant ที่รอดในรอบแรก ยุบเป็นรูเชิงตรรกะได้ตามนี้ **ทุกแถวคือบั๊กที่ merge ผ่านได้จริง
ตอน coverage 99%**:

| # | แก้โค้ดอย่างไร | พังยังไงในโปรดักชัน | ทำไมเทสเดิมไม่จับ | เทสที่อุด |
|---|---|---|---|---|
| 1 | ลบ `j.RunAt = now.Add(delay)` ใน `retryLocked` | **backoff ถูกเพิกเฉยทั้งระบบ** — DB ที่ล่มโดนถล่มซ้ำทันที | เทสเดิม `Nack` ด้วย `delay=0` ทุกที่ | `TestNackDelayIsHonored` |
| 2 | ลบ `promoteLocked()` ออกจาก `Ack`/`Nack`/`Extend`/`Stats` | พฤติกรรมขึ้นกับว่ามีใครเรียก `Dequeue` คั่นหรือไม่ ([I2c](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)) | ทุกเทสมี `Dequeue` คั่นอยู่แล้วโดยบังเอิญ | `TestPromoteBeforeEveryDecision` |
| 3 | ลบ `j.enqueued = time.Now()` | `OldestReady` โกหก — **มองไม่เห็นงานที่ค้างมา 3 ชั่วโมง** ([§6.1](#61-metric-ที่ต้อง-alert-คือ-อายุงานที่เก่าที่สุด-ไม่ใช่-ความลึกคิว)) | ไม่มีเทสไหนแตะ `OldestReady` เลย | `TestStatsOldestReady`, `TestOldestReadySurvivesRetry` |
| 4 | ลบ `heap.Fix` ใน `Extend` | lease heap เรียงผิด → งานที่ต่ออายุแล้วบังหน้างานที่หมดจริง | เทสเดิมมีงาน inflight **ตัวเดียว** — heap 1 ตัวเรียงถูกเสมอ | `TestExtendReordersLeaseHeap` |
| 5 | ลบ `heap.Remove(&q.leases, ...)` ใน `Nack` | [I2](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา) พัง: งานอยู่ทั้งใน `ready` และ `leases` → ถูกแจกซ้ำถาวร | ไม่มีใครตรวจโครงสร้างภายในหลัง `Nack` | `checkInvariants` |
| 6 | ลบ `q.mu.Unlock()` ในสาขา `closed` ของ `Dequeue` | **deadlock ทั้งคิว** ตอน shutdown | เทสเดิมเจอ `ErrClosed` แล้วจบ ไม่เคยแตะคิวต่อ | `TestClosedQueueSemantics` |
| 7 | ลบ `broadcastLocked()` ใน `Enqueue`/`Nack` | worker หลับทั้งที่มีงานรอ — latency พุ่งจาก 0 เป็นเท่ากับ visibility timeout | ทุกเทส `Enqueue` **ก่อน** `Dequeue` → ไม่เคยมี waiter จริง | `TestBlockedDequeueIsWoken` |
| 8 | ลบ `timer.Reset(d)` ใน `Dequeue` | waiter ที่ **แพ้การแย่งงาน** หลับตลอดกาล | ต้องมี waiter ≥2 ตัวถึงจะมีคนแพ้ | `TestLosingWaiterRearmsTimer` |
| 9 | ลบ `q.Ack(j.ID)` / `q.Nack(...)` ใน `RunPool` | งานถูกทำซ้ำทุกครั้งที่ lease หมด / ไม่มีวันถึง DLQ | เทสเดิมตั้ง visibility สั้น — **lease หมดอายุกลบร่องรอยให้พอดี** | `TestRunPoolAcksAndNacks` (ตั้ง lease = 1 ชม.) |
| 10 | ลบ `wg.Wait()` ใน `RunPool` | `RunPool` คืนค่าขณะงานยังทำอยู่ = graceful shutdown เป็นคำโกหก | ไม่มีเทสไหนตรวจว่า worker ออกหมดจริง | `TestRunPoolWaitsForWorkers` |
| 11 | `100 * time.Millisecond` → `100 / time.Millisecond`, `<<` → `>>` | backoff เป็น ~0 เสมอ → retry ถล่มปลายทางที่กำลังฟื้น | `TestBackoffBounds` เช็กแค่ "อยู่ในช่วง [0, ceiling]" ซึ่ง **0 ก็อยู่ในช่วง** | `TestBackoffGrowsAndSaturates` |
| 12 | `capacity < 1` → `<= 1`, `visibility <= 0` → `<= 1` | คิวปฏิเสธ config ที่ถูกต้องตอนบูต | เทสเดิมยืนยันแค่ "ค่าผิดต้อง panic" ไม่เคยยืนยัน "ค่าถูกต้องไม่ panic" | `TestConfigBoundaryAccepted` |
| 13 | ลบ `q.ackTooLate++` ใน `Ack`/`Nack` | SLI ที่บอกว่า visibility สั้นไป เงียบไปครึ่งหนึ่ง | เทสเดิมเช็กเฉพาะเส้นทาง `Extend` | `TestAckTooLateCountsEveryPath` |
| 14 | ลบ `h.jobs[n-1] = nil` ใน `Pop` | backing array ถือ `*Job` ค้าง → **RAM ค้างที่ระดับ peak ตลอดไป** ทั้งที่ `Stats` บอกคิวว่าง | สังเกตจากพฤติกรรมไม่ได้ ต้องดูที่การถือหน่วยความจำ | `TestPoppedJobNotRetained` (`weak.Pointer` + `runtime.GC`) |

> **บทเรียนที่ซ้ำที่สุดในตารางนี้:** เทสหลายตัว "ผ่านด้วยเหตุผลที่ผิด" —
> lease หมดอายุกลบผลของ `Ack` ที่หายไป, timer กลบผลของ `broadcast` ที่หายไป,
> heap ที่มีสมาชิกตัวเดียวกลบผลของ `heap.Fix` ที่หายไป
> **ทางแก้ไม่ใช่เพิ่มเทส แต่คือตัดกลไกสำรองออกจากสมการ**: ตั้ง visibility = 1 ชั่วโมง
> ให้ "งานกลับมาเอง" เป็นไปไม่ได้, เทียบเวลาที่ตื่นแบบ `== 0` ไม่ใช่ "ตื่นในที่สุด"

### 8.3b เพดานของ mutation testing — และบั๊กจริงสองตัวที่มันมองไม่เห็น

`tools/mutate` แก้โค้ดได้เฉพาะแบบที่ **operator ของมันรู้จัก**: สลับตัวเปรียบเทียบ,
ลบ statement, แก้ค่าคงที่ บั๊กที่ต้อง "เขียนโค้ดเพิ่ม" หรือ "สลับตัวแปรคนละตัว" ถึงจะเกิด
จะไม่มีวันโผล่ในรายงาน — **mutation score 100% ไม่ใช่ใบอนุญาตให้เลิกอ่านโค้ด**

รอบที่สองจึงเป็นการรีวิวโดยตรงเทียบกับสเปกใน §1–§6 ผลคือ **บั๊กจริงในโค้ดโปรดักชัน 2 ตัว**
(ไม่ใช่แค่รูของเทส) และรูอีก 4 จุดที่ operator เอื้อมไม่ถึง:

| # | สิ่งที่พบ | ผลจริง | ทำไม mutation มองไม่เห็น | เทส |
|---|---|---|---|---|
| **B1** 🐞 | **`Extend` ที่ *ย่น* lease ไม่ปลุก waiter** | worker ที่รู้ตัวว่าจะเสร็จเร็วกว่าที่ขอ คืนงานเร็วไม่ได้ — งานถูกส่งซ้ำตอน lease **เดิม** หมด (1 ชม. แทน 1 วิ) | ต้อง **เพิ่ม** `broadcastLocked()` ถึงจะเห็น ไม่ใช่ลบอะไรออก | `TestExtendShorteningWakesWaiter` |
| **B2** 🐞 | **`Dead()` ไม่ `promoteLocked` ก่อนตอบ** — ละเมิด [I2c](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา) ที่ §2.4 ประกาศไว้เอง | `Dead()` กับ `Stats().Dead` ตอบไม่ตรงกันในวินาทีเดียวกัน ขึ้นกับว่าใครถูกเรียกก่อน | เป็นบรรทัดที่ **ขาดหายไป** — ไม่มีอะไรให้กลายพันธุ์ | `TestDeadPromotesBeforeReporting` |
| 3 | `safeCall` ส่ง `ctx` ตัวนอกแทน `cctx` | timeout ต่องานไม่ทำงาน → งานที่ค้างกินสล็อต worker ตลอดกาล | `cctx, cancel := ...` เป็น `:=` — ลบแล้วคอมไพล์ไม่ผ่าน จึงถูกนับเป็น invalid | `TestHandlerDeadlineEnforced` |
| 4 | `promoteLocked` ระบายทีละตัวแทนที่จะระบายหมด | งาน delayed 10k ตัวที่ถึงเวลาพร้อมกัน ต้องรอ 10k รอบกว่าจะระบายหมด — ดูเหมือนคิวค้าง | ต้องเปลี่ยน `for` เป็น `if` ซึ่งไม่ใช่ operator ที่มี | `TestPromoteDrainsEverythingDue` |
| 5 | tie-break ใช้ `enqueued` แทน `seq` | **replay DLQ 10k ตัวแซงหน้า traffic สดทั้งหมด** (I4 บอกว่า `enqueued` ไม่ถูกรีเซ็ต → งาน replay เก่าที่สุดเสมอ) | ต้องเปลี่ยนไปใช้ฟิลด์คนละตัว | `TestFIFOTieBreakIsSeqNotAgeOrAttempt` |
| 6 | tie-break ใช้ `Attempt` แทน `seq` | งานที่ล้มบ่อยได้คิวหน้าเรื่อย ๆ = starvation กลับด้าน | เหมือนข้อ 5 | เดียวกัน |
| 7 | `jobHeap.Pop` ไม่ตั้ง `index = -1` | sentinel หายไป — `heap.Remove` ตัวถัดไปอาจแตะงานผิดตัว | `retryLocked` เขียนค่าเดียวกันซ้ำให้ → สอง statement ครอบกันเอง เทสที่ผ่าน `MemQueue` แยกไม่ออก | `TestHeapPopClearsIndex` (เทส `jobHeap` ตรง ๆ) |

**B1 กับ B2 แก้แล้วทั้งคู่** และมีเทสเฝ้าไว้ทั้งคู่ — B2 คือหนึ่งบรรทัด
(`q.promoteLocked(time.Now())` ใน `Dead`) ส่วน B1 ต้องมีเงื่อนไข:

```go
prev := j.leaseUntil
j.leaseUntil = time.Now().Add(d)
heap.Fix(&q.leases, j.index)
if j.leaseUntil.Before(prev) {   // ★ เฉพาะตอน "ย่น" เท่านั้น
	q.broadcastLocked()
}
```

**เวอร์ชันแรกของการแก้เรียก `broadcastLocked()` ทุกครั้ง — ซึ่งผิด**
`Extend` คือ heartbeat ของงานยาว (ทุก `visibility/3` ตาม [§6.4](#64-งานที่ยาวกว่า-visibility-timeout-จะถูกทำซ้ำตลอดกาล))
จึงเป็น path ที่ถูกเรียกถี่ที่สุด และ broadcast ปลุก waiter **ทุกตัว** ([§7](#7-รีวิว-ข้อจำกัดที่รู้ตัว) ข้อ 6):

`BenchmarkExtendHeartbeat*` ใน `bench_test.go` (Apple M4, 10 cores, งาน inflight 64 ตัว):

| waiter ที่บล็อกอยู่ | broadcast ทุกครั้ง | เฉพาะตอนย่น |
|---|---|---|
| 0 | 134.5 ns/op · 1 alloc | **113.8 ns/op · 0 alloc** |
| 64 | 1,004 ns/op · 1 alloc | **114.3 ns/op · 0 alloc** |
| 512 | 2,177 ns/op · 1 alloc | **114.7 ns/op · 0 alloc** |

benchmark นี้ถูก**เพิ่มเข้า repo** ไม่ใช่รันทิ้ง — ตัวเลขที่อ้างในคอมเมนต์ต้องผลิตซ้ำได้ด้วย
`make bench` ไม่งั้นมันคือความรู้สึกที่ใส่หน่วย ns/op

การ**ต่อ** lease ให้ยาวขึ้นไม่ต้องปลุกใคร: waiter ตื่นที่เวลาเดิม เห็นว่ายังไม่ถึงคิว
แล้วตั้ง timer ใหม่เอง — เสีย wake เปล่าหนึ่งครั้ง ไม่ใช่ความผิดพลาด
มีแต่การ**ย่น**เท่านั้นที่เลื่อน event ให้มาก่อน timer ที่ตั้งไว้แล้ว

> **บทเรียนของ §8 ที่ย้อนกลับมาหาตัวเอง:** การแก้บั๊กที่ถูกต้องเชิงตรรกะ แต่ทำให้ path
> ที่ร้อนที่สุดช้าลง 19 เท่า **ไม่มีเทสตัวไหนใน repo นี้จับได้** — coverage 100%,
> mutation 100%, `-race` 20 รอบ, fuzz: เขียวสนิททั้งก่อนและหลัง
> ต้องเขียน benchmark ที่จำลองรูปแบบการใช้จริง (งานยาว heartbeat + waiter บล็อกจำนวนมาก)
> ถึงจะเห็น
>
> **ประตูคุณภาพวัดความถูกต้อง ไม่ได้วัดว่าของยังเร็วอยู่ไหม** — และมันไม่ได้แทนการรีวิว
> มันแค่กันของที่แย่กว่านั้นไม่ให้หลุดเข้ามา

> **B1 ยังทำให้เหตุผลใน `mutation-allow.txt` ถูกต้องขึ้นด้วย:** ข้ออ้างว่า
> "broadcast ใน `promoteLocked` ซ้ำซ้อน" ตั้งอยู่บนสมมติฐานว่า *ทุกอย่างที่ขยับ timeline
> ให้เร็วขึ้นจะ broadcast เอง* ซึ่ง**ไม่จริง**ก่อนแก้ B1 — การพิสูจน์ความสมมูลผิดพลาด
> เพราะสมมติฐานที่ไม่ได้ตรวจ นี่คือเหตุผลที่ทุกบรรทัดใน allowlist ต้องเขียนเงื่อนไขกำกับไว้
> ไม่ใช่แค่คำว่า "equivalent"

**บทเรียน:** ใช้ mutation score เป็น**พื้น** ไม่ใช่**เพดาน** — มันบอกว่าเทสอ่อนตรงไหน
แต่ไม่เคยบอกว่าโค้ด**ขาด**อะไร

### 8.3c เครื่องมือที่ตัดสินคนอื่น ต้องถูกตัดสินก่อน

`tools/mutate` เป็นตัวที่ออกใบรับรอง "100%" ให้ทั้ง repo — ถ้ามัน**ตัดสินผิด** ตัวเลขทุกตัว
ในบทนี้เป็นเรื่องโกหก และไม่มีอะไรจะจับได้เลย ความล้มเหลวที่อันตรายที่สุดคือแบบเงียบ:

| ถ้าตัดสินผิดแบบนี้ | ผลคือ |
|---|---|
| นับ **build failure** เป็น `killed` | mutant ที่เขียนเป็นโค้ดจริงไม่ได้ ถูกนับเป็นความสำเร็จ → score พองขึ้นฟรี ๆ |
| นับ **timeout** เป็น `invalid` | mutant ที่ทำให้ระบบค้าง (ลบ `Attempt++`) หายไปจากตัวหาร |
| baseline แดงแล้วยังรันต่อ | ทุก `survived` กลายเป็นขยะ เพราะเทสแดงอยู่แล้วตั้งแต่ต้น |
| allowlist entry ที่ไม่ match อะไรเลย | คนอ่านเชื่อว่ายังมีเหตุผลคุ้มครองอยู่ ทั้งที่ยกเว้น "อะไรก็ไม่รู้" |

จึงมี `tools/mutate/main_test.go` (551 บรรทัด, coverage **100.0%**) ที่เทส**การตัดสิน**
เป็นหลัก ไม่ใช่แค่ว่าโปรแกรมรันผ่าน — ด้วย fixture สามชุดใน `testdata/`:

| fixture | เทสของมัน | ผลที่ต้องได้ |
|---|---|---|
| `strong` | assert ครบทุกเคส รวม `Larger(3,3)` ที่แยก `>` ออกจาก `>=` | score **100%**, และ `invalid > 0` (mutant `+` บน string) |
| `weak` | เรียกฟังก์ชันแต่**ไม่ assert อะไรเลย** → coverage 100% | ต้องมี survivor และ score **< 100%** |
| `broken` | โค้ดผิดตั้งแต่แรก baseline แดง | ต้องปฏิเสธก่อนวัดอะไรทั้งสิ้น |

> **fixture `strong` ตอนแรกใช้ `Max(a,b) int` แล้วเทสแดง** เพราะ `>` กับ `>=` ให้ผลเหมือนกันเป๊ะ
> เมื่อ `a == b` (คืนค่าเท่ากันทั้งสองทาง) — มันคือ equivalent mutant ที่ไม่มีเทสไหนฆ่าได้
> ต้องเปลี่ยนเป็น `Larger(a,b) string` ที่คืน `"a"`/`"b"` ถึงจะแยกออก
> **fixture ที่ใช้พิสูจน์ว่า "เทสแน่นแล้ว mutant ตายหมด" ต้องไม่มี mutant ที่ฆ่าไม่ได้ปนอยู่**

เขียนเทสชุดนี้แล้วเจอบั๊กจริงในเครื่องมือเพิ่มอีกหนึ่งตัว: **`-jobs 0` ทำให้ค้างตลอดกาล**
(ไม่มี worker รับงาน แต่ยังป้อนงานเข้า channel) — ตอนนี้ถูกดันขึ้นเป็น 1 และมีเทสเฝ้า

อีกตัวเจอจากการทดลอง kill กลางรัน: **โปรเซสถูกฆ่า = `defer` ไม่ทำงาน** → sandbox
ค้างใน temp dir ทุกครั้ง (jobs × ~130KB สะสมเรื่อย ๆ) — แก้ด้วย signal handler
(SIGINT/SIGTERM → เก็บกวาด sandbox แล้วออกด้วย code 130) และมีเทสที่ยิงสัญญาณจริงเฝ้าไว้

**ผลข้างเคียงที่ดีของการไล่ให้ถึง 100%:** สองจุดที่เทสเอื้อมไม่ถึงถูก**เขียนใหม่ให้ง่ายลง**
แทนที่จะเพิ่มโครงนั่งร้านเข้าไปเพื่อเอื้อม — `newSandbox` รวม read+write เป็น error เดียว,
และ worker เลิกสร้าง sandbox เองกลางรัน (สร้างครบตั้งแต่ต้นแล้วส่งเข้าไป)
**โค้ดที่เทสยากมักเป็นโค้ดที่ซับซ้อนเกินจำเป็น — 100% บังคับให้เห็นข้อนั้น**

### 8.4 Invariant checker + model-based + fuzz

เทสตามตัวอย่างพิสูจน์ได้แค่ลำดับเหตุการณ์ที่คนเขียนเทส**นึกออก**
บั๊กของคิวคือบั๊กเรื่อง**ลำดับ** — `heap.Remove` ด้วย index ที่ค้างจะลบงานคนอื่นทิ้ง
**เงียบ ๆ**: ไม่มี error, ไม่มี panic, มีแค่งานที่หายไปหนึ่งชิ้น

**ชั้นที่ 1 — `checkInvariants(t, q, "หลังทำอะไร")`** ตรวจทุกข้อใน [§2.4](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา) ที่ตรวจได้จากภายใน:

| ตรวจอะไร | พังแล้วเกิดอะไร |
|---|---|
| `h.jobs[i].index == i` ทุก heap | `heap.Remove/Fix` ไปแตะงานผิดตัว |
| heap order: ลูกไม่น้อยกว่าพ่อ | ลำดับ priority/เวลาเพี้ยนโดยไม่มีสัญญาณ |
| **I1** งานหนึ่งชิ้นอยู่ได้ที่เดียว (เทียบ pointer ไม่ใช่ ID) | งานถูกแจกซ้ำถาวร |
| **I2** `id ∈ inflight ⟺ job ∈ leases` | งานค้าง inflight ตลอดกาล (memory leak) |
| **I2b** `ids` = `ready ∪ delayed ∪ inflight` พอดี | ID รั่ว → enqueue ใหม่ไม่ได้ตลอดกาล |
| **I6** `sizeLocked()` ถูกต้อง และ ≤ `capacity` | รับงานเกินที่ระบบทำไหว |
| **I4** ทุกงานใน `ready` มี `enqueued` | `OldestReady` คำนวณผิด |

**ชั้นที่ 2 — กฎอนุรักษ์ (conservation law)** จับสิ่งที่ invariant เชิงโครงสร้างจับไม่ได้:
งานที่หายไปทั้งที่ทุกโครงสร้าง "ถูกต้องในตัวมันเอง"

```
enqueued == acked + dead + deadDropped + (ready + delayed + inflight)
```

**ชั้นที่ 3 — `TestModelRandomOps`**: สุ่ม 20 seed × 400 operation
(`Enqueue`/`Dequeue`/`Ack`/`Nack`/`Extend`/เวลาเดิน/`Dead`) แล้วตรวจสองชั้นบน
**หลังทุก operation**. seed คงที่ ⇒ เทสแดงแล้ว reproduce ได้ทันที ไม่ใช่ "ลองรันใหม่ดู"
รันในนาฬิกาปลอม ⇒ 8,000 operation จบใน ~0 วินาที

**ชั้นที่ 4 — `FuzzQueueOps`**: ให้ fuzzer หาลำดับที่เราไม่ได้นึกถึง
ใช้ driver ตัวเดียวกัน แต่ลำดับ operation มาจาก `[]byte` ที่ fuzzer ป้อน

```bash
make fuzz                     # 30 วิ ≈ 600k execution
```

input ที่ทำให้พังจะถูกเขียนลง `testdata/fuzz/` อัตโนมัติ **แล้วกลายเป็นเทสถาวร**
— บั๊กที่เจอครั้งเดียวจะไม่มีวันกลับมาโดยไม่มีใครรู้

**ชั้นที่ 5 — `TestConcurrentStress`**: producer 4 + worker 8 + observer 1 ยิงทับกันจริง
ใต้ `-race` ด้วย `visibility = 2ms` (lease หมดกลางคันตลอดเวลาโดยตั้งใจ)
จบแล้วตรวจ invariant + กฎอนุรักษ์ นี่คือเทสเดียวที่บังคับให้ `Ack`/`Nack`/`Extend`/
`Enqueue`/`Stats`/`Close` ทำงานทับกันจริง

### 8.5 ความครอบคลุมของสเปก

ตาราง [§2.3](#23-ตารางการเปลี่ยนสถานะแบบครบถ้วน) มี 20 ช่อง — `TestSpecStateMatrix` เดินครบทุกช่อง
รวมถึงช่องที่ "เป็นไปไม่ได้" ซึ่งเป็นที่ที่บั๊กชอบซ่อน:

| สถานะตั้งต้น | `Enqueue` (ID เดิม) | `Dequeue` | `Ack` | `Nack` | เวลาผ่านไป |
|---|---|---|---|---|---|
| *(ไม่มี)* | ✅ ผ่าน | ✅ บล็อก | ✅ `ErrNotInflight` | ✅ `ErrNotInflight` | — |
| **DELAYED** | ✅ `ErrDuplicateID` | ✅ บล็อก | ✅ `ErrNotInflight` | ✅ `ErrNotInflight` | ✅ → READY ที่ `RunAt` พอดี |
| **READY** | ✅ `ErrDuplicateID` | ✅ → INFLIGHT, `Attempt=1` | ✅ `ErrNotInflight` | ✅ `ErrNotInflight` | — |
| **INFLIGHT** | ✅ `ErrDuplicateID` | ✅ บล็อก (ถูกซ่อน) | ✅ → DONE | ✅ → DELAYED/DLQ | ✅ → READY ที่ `leaseUntil` พอดี |
| **DLQ** | ✅ ผ่าน (ID ถูกปล่อย) | ✅ บล็อก | ✅ `ErrNotInflight` | ✅ `ErrNotInflight` | — |

และหัวข้อที่เหลือ:

| หัวข้อ | เทสอะไร | เทส |
|---|---|---|
| ลำดับ | priority + FIFO ภายในชั้นเดียวกัน | `TestPriorityThenFIFO`, `TestFIFOStrictUnderManyEqualPriority` (200 งาน) |
| เวลา | งาน delayed ไม่ออกก่อนเวลา (เป๊ะ) | `TestDelayedJobNotServedEarly` |
| ทนความล้มเหลว | lease หมด → ส่งซ้ำ, `Attempt` เพิ่ม, ack ซ้ำล้ม | `TestLeaseExpiryRedelivers` |
| จุดจบ | retry จนครบ → DLQ ไม่วนตลอดกาล | `TestRetryUntilDLQ`, `TestDLQIsBounded` |
| ขอบเขต | คิวเต็ม reject, inflight นับเป็นความจุ | `TestFullQueueRejects` |
| การปลดบล็อก | `ctx` cancel / `Close` / งานใหม่ / lease หมด | `TestDequeueUnblocks`, `TestBlockedDequeueIsWoken`, `TestCloseWakesEveryWaiter` (64 waiter) |
| backoff | อยู่ในช่วง **และโตจริง และอิ่มตัว และกระจาย** | `TestBackoffBounds`, `TestBackoffGrowsAndSaturates` |
| ความเป็นเจ้าของ | `Dequeue`/`Dead()` คืนสำเนา ไม่ใช่ตัวจริง | `TestDequeueReturnsIsolatedCopy`, `TestDeadJobsReplayCleanly` |
| shutdown | `ctx` ที่ยกเลิกแล้วต้องไม่กินโควตา retry | `TestCancelledCtxTakesNoJob` |
| ทรัพยากร | goroutine/timer ไม่รั่ว, heap ไม่ถือ pointer ค้าง | `TestNoGoroutineLeak`, `TestPoppedJobNotRetained` |
| ครบวงจร | pool + panic + retry-แล้วผ่าน + graceful shutdown | `TestPoolEndToEnd`, `TestRunPoolAcksAndNacks` |
| timeout ต่องาน | handler ได้ `ctx` ที่มี deadline และถูกตัดตรงเวลา | `TestHandlerDeadlineEnforced` |
| lease | `Extend` ทั้งต่อและ**ย่น**; ย่นแล้วต้องปลุก waiter | `TestExtendReordersLeaseHeap`, `TestExtendShorteningWakesWaiter` |
| promote | ระบาย **ทุกตัว** ที่ถึงเวลาในครั้งเดียว | `TestPromoteDrainsEverythingDue` |
| tie-break | เรียงตาม `seq` ไม่ใช่อายุและไม่ใช่ `Attempt` (replay DLQ ต้องต่อท้าย) | `TestFIFOTieBreakIsSeqNotAgeOrAttempt` |
| สัญญาของ heap | `Pop`/`Remove` ต้องตั้ง `index = -1` | `TestHeapPopClearsIndex` |
| DLQ | `Dead()` ต้องตอบตรงกับ `Stats().Dead` เสมอ | `TestDeadPromotesBeforeReporting` |

**invariant เดียวที่เทสอัตโนมัติไม่ได้คือ [I5](#24-invariant-ที่ต้องเป็นจริงตลอดเวลา)** ("ไม่ถือ mutex ขณะเรียก handler") —
เป็นคุณสมบัติเชิงโครงสร้างของโค้ด ไม่ใช่ของ state. บังคับด้วยการรีวิว:
`Dequeue` ต้อง `Unlock` ก่อน `return` และ `RunPool` ต้องไม่เรียก handler ใต้ล็อกใด ๆ

### 8.6 บังคับใน CI — ทั้งหมดเป็น "ประตู" ไม่ใช่ "รายงาน"

```bash
make ci      # lint + race + cover + mutate — แดงตัวเดียวก็ห้าม merge
```

| ประตู | คำสั่ง | เกณฑ์ | ทำไมต้องเป็นประตู |
|---|---|---|---|
| lint | `make lint` | `gofmt`/`go vet`/`staticcheck`/`govulncheck` ไม่มี output | ถูกที่สุด เร็วที่สุด |
| race | `make race` | เขียว | **`-race` ไม่ใช่ทางเลือกสำหรับโค้ดคิว** — บั๊กที่เหลือทั้งหมดเป็นบั๊กเรื่องลำดับ |
| coverage | `make cover` | ≥ **100%** | ที่ขนาดนี้ 100% ทำได้จริง; ตั้ง 80% แล้วจะไม่มีใครรู้ว่า 20% ที่หายคืออะไร |
| mutation | `make mutate` | ≥ **100%** (นับเฉพาะที่ไม่อยู่ใน allowlist) | ประตูที่สำคัญที่สุด — coverage พิสูจน์แค่ "รันผ่าน" |
| flaky | `make flaky` | เขียว 20 รอบติด | เทสที่ผ่านบ้างไม่ผ่านบ้างแย่กว่าเทสที่ไม่มี |
| fuzz | `make fuzz` | 30–60 วิ ต่อ PR | ของยาวปล่อยเป็น nightly; crasher ถูกเก็บเป็น artifact |

**ตั้งเกณฑ์ที่ 100% ไม่ใช่ความสมบูรณ์แบบ แต่เป็นการทำให้ข้อยกเว้นต้องมีชื่อ:**
ทุกอย่างที่ต่ำกว่า 100% ต้องมีบรรทัดใน `mutation-allow.txt` พร้อมเหตุผล
ต่างจากเกณฑ์ 85% ที่ยอมให้ 15% เป็นความมืดที่ไม่มีใครต้องอธิบาย

**Coverage ขึ้น [Codecov](https://app.codecov.io/gh/Kidpech-code/go-queue) ผ่าน OIDC — ไม่มี `CODECOV_TOKEN` อยู่ในระบบเลย:**
repo เป็น public และ `codecov-action@v5` รองรับการยืนยันตัวตนด้วย OIDC: job `cover`
ประกาศ `permissions: id-token: write` เพื่อขอ token อายุสั้นจาก GitHub ต่อการรันหนึ่งครั้ง
แล้ว Codecov ตรวจกับ GitHub เอง — จึงไม่มี secret ให้ตั้งผิดที่ ให้หมุนตามรอบ หรือให้รั่ว
(secret ที่ไม่มีอยู่ คือ secret ที่ปลอดภัยที่สุด)

สองการตัดสินใจที่ทำให้ตัวเลขบน badge เชื่อถือได้:

- **โปรไฟล์ที่อัปโหลดคือไฟล์เดียวกับที่ประตู `make cover` ใช้** — ตัวเลขบนเว็บกับใน CI
  จึงตรงกันเสมอ ไม่มีทางที่ badge จะเขียวในขณะที่ประตูแดง
- **`fail_ci_if_error: true`** — อัปโหลดล้มคือ CI แดงที่มองเห็น ไม่ใช่กราฟบน Codecov
  ที่ค้างของเก่าเงียบ ๆ แล้วทุกคนเข้าใจว่ายังอัปเดตอยู่ (ประตูที่ล้มแบบเงียบไม่ใช่ประตู — §8.6
  ทั้งหัวข้อนี้มีไว้เพื่อประโยคเดียวนั้น)

> **ราคาที่จ่าย:** `queue.go` 498 บรรทัด ↔ เทส 2,097 บรรทัด (115 test case)
> ↔ เครื่องมือ 548 บรรทัด + เทสของเครื่องมือ 622 บรรทัด (อัตราส่วน 1 : 4.2 : 2.3). `make mutate` ใช้เวลา ~2 นาทีบน M4 10 cores.
> สำหรับโค้ดที่ถ้าพังแล้วงานของลูกค้าหาย — นี่คือราคาที่ถูก

---


## 9. ต่อเข้าของจริง — `main.go` เต็มรูปแบบ

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kidpech-code/go-queue"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// จังหวะ 1: ctx ที่ผูกกับ SIGTERM — stdlib ล้วน ไม่ต้องเขียน signal handler เอง
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// capacity/worker/VT มาจาก §6.2 ไม่ใช่จากการเดา
	q := queue.NewMemQueue(1_000, 5, 30*time.Second)

	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{Name: "queue_oldest_ready_seconds"},
		func() float64 { return q.Stats().OldestReady.Seconds() },
	))
	go http.ListenAndServe(":9090", promhttp.Handler())

	pool := make(chan struct{})
	go func() {
		defer close(pool)
		queue.RunPool(ctx, q, 36, 20*time.Second, handle)
	}()

	<-ctx.Done()
	slog.Info("ได้รับสัญญาณหยุด — กำลัง drain")

	// worker หยุดรับงานใหม่ทันทีที่ ctx ถูกยกเลิก (Dequeue เช็ก ctx ก่อน ready)
	// งานที่เหลือใน ready/delayed จึงคงอยู่ครบ ไม่ถูกกวาดมาเผา Attempt ทิ้ง
	select {
	case <-pool:
		slog.Info("drain สำเร็จ", "งานที่ยังไม่ได้ทำ", q.Stats().Ready)
	case <-time.After(25 * time.Second):   // < terminationGracePeriodSeconds
		s := q.Stats()
		// MemQueue อยู่ในหน่วยความจำ: process ตาย = งานที่ค้างหายหมด (§7 ข้อ 2)
		// ต้องการ "ส่งซ้ำหลัง lease หมด" จริง ๆ ⇒ ต้องเป็น Postgres (§5)
		slog.Warn("drain timeout — งานที่ค้างจะหายไปกับ process",
			"inflight", s.Inflight, "ready", s.Ready, "delayed", s.Delayed)
	}
}

type emailJob struct {
	V    int    `json:"v"`      // §5.4 ข้อ 5 — version ตั้งแต่วันแรก
	To   string `json:"to"`
	Body string `json:"body"`
}

func handle(ctx context.Context, j *queue.Job) error {
	var e emailJob
	if err := json.Unmarshal(j.Payload, &e); err != nil {
		return err                       // payload พัง → retry แล้วลง DLQ ตามระบบ
	}
	if e.V != 1 {
		return fmt.Errorf("payload version %d ที่ไม่รองรับ", e.V)
	}
	// §6.3 — j.ID เป็น idempotency key ที่ปลายทาง
	return sendEmail(ctx, e.To, e.Body, j.ID)
}
```

**สิ่งที่ตัวอย่างนี้แสดงครบ:** `signal.NotifyContext` (stdlib), metric ตัวที่สำคัญที่สุด,
graceful shutdown 2 จังหวะที่ timeout เรียงถูก, payload versioning, idempotency key,
และการปล่อยให้กลไก retry/DLQ จัดการ error เอง แทนที่จะ log แล้วกลืน

---

## ภาคผนวก: คำศัพท์

| คำ | ความหมายในเอกสารนี้ |
|---|---|
| **at-least-once** | งานถูกทำอย่างน้อย 1 ครั้ง อาจมากกว่า — semantics ที่ repo นี้ให้ |
| **backpressure** | แรงต้านที่ย้อนกลับไปหาผู้ผลิตเมื่อผู้บริโภคตามไม่ทัน |
| **claim check** | เก็บ payload ใหญ่นอกคิว (S3) แล้วส่งแค่ key |
| **DLQ** | Dead Letter Queue — ที่พักงานที่ล้มครบจำนวนครั้งแล้ว รอมนุษย์ |
| **fencing token** | เลขที่โตขึ้นเรื่อย ๆ ใช้กันไม่ให้ผู้ถือ lease เก่าเขียนทับของใหม่ |
| **head-of-line blocking** | งานหัวคิวช้าทำให้งานหลังทั้งหมดรอ |
| **idempotent** | ทำซ้ำแล้วผลลัพธ์เหมือนเดิม |
| **inflight** | งานที่ worker ถืออยู่ (ถือ lease) ยังไม่ ack |
| **Little's Law** | `L = λW` — ใช้หาจำนวน worker และความจุคิว |
| **poison pill** | งานที่ทำยังไงก็ล้ม ต้องมี DLQ มารับ |
| **spurious wakeup** | ถูกปลุกทั้งที่เงื่อนไขยังไม่จริง — ต้องมี `for` ครอบเสมอ |
| **thundering herd** | ทุกคนกลับมาพร้อมกันหลังปลายทางฟื้น → ล้มซ้ำ |
| **transactional outbox** | เขียน business data + งาน ใน transaction เดียว |
| **visibility timeout (VT)** | ระยะที่งานถูกซ่อนหลัง dequeue; หมดแล้วโผล่กลับมา |
| **WFQ** | Weighted Fair Queuing — แบ่งทรัพยากรตามน้ำหนักที่กำหนด |

---

## อ้างอิง

- AWS Architecture Blog — *Exponential Backoff and Jitter* (ที่มาของ full jitter — [§4.5](#45-exponential-backoff--full-jitter))
- Martin Kleppmann — *How to do distributed locking* (fencing token — [§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ))
- PostgreSQL Docs — `SELECT ... FOR UPDATE SKIP LOCKED` (PG 9.5+), TOAST
- Go 1.23 release notes — timer/ticker channel เปลี่ยนเป็น unbuffered
- Go 1.25 release notes — `testing/synctest` เป็น stable
- `runtime/chan.go`, `runtime/select.go` — `hchan`, `pollorder`/`lockorder` ([§3.3](#33-โครงสร้างข้อมูลในหน่วยความจำ))
- Kafka — hierarchical timing wheel (ทางเลือกแทน heap เมื่อ timer หลักล้าน — [§4.2](#42-delay-queue--min-heap--timer-ตัวเดียว))
- Facebook Engineering — *CoDel + adaptive LIFO* ([§1.5](#15-bounded-เสมอ--และ-4-นโยบายเมื่อเต็ม))

---

## สรุปสั้น

1. **เริ่มที่ `chan` เสมอ** — ~60% ของงานจบตรงนั้น
2. ต้องการ **ทน process ตาย** → Postgres `SKIP LOCKED` (ได้ transactional outbox แถม) ไม่ใช่ Kafka
3. **at-least-once + idempotent consumer** เท่านั้น — "exactly-once" ข้ามระบบไม่มีอยู่จริง
   และ [§2.6](#26-ปัญหาที่แก้ไม่ได้-ack-แข่งกับ-lease-หมดอายุ) อธิบายว่าทำไมปิดช่องไม่ได้
4. อัลกอริทึมที่ต้องทำถูก 5 ตัว: heap ที่มี tie-break, timer ตัวเดียวจาก heap head,
   condvar-ที่-`select`-ได้, lease + `Attempt++` ตอน dequeue, backoff + full jitter
5. **alert ที่ `oldest_ready_seconds` ไม่ใช่ depth**; ตั้งจำนวน worker จาก `λ×W ÷ 0.7`
6. **ทุกตัวเลขมีที่มา** — capacity, worker, VT คำนวณจาก [§6.2](#62-หาจำนวน-worker-ด้วย-littles-law-อย่าเดา) ไม่ใช่เดา
