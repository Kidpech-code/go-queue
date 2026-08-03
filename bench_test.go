package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// benchmark สำหรับตอบสองคำถามด้วยตัวเลข ไม่ใช่ความรู้สึก:
//  1. "broadcast ปลุก waiter ทุกตัว" แพงจริงแค่ไหนเมื่อ worker เยอะ
//  2. feature ทั้งชุด (heap/priority/lease/DLQ) คิดราคาเท่าไหร่เทียบกับ chan เปล่า

func benchThroughput(b *testing.B, workers int) {
	b.Helper()
	q := NewMemQueue(b.N+workers+1, 3, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	done := make(chan struct{}, b.N)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, err := q.Dequeue(ctx)
				if err != nil {
					return
				}
				q.Ack(j.ID)
				done <- struct{}{}
			}
		}()
	}

	b.ResetTimer()
	for i := range b.N {
		q.Enqueue(&Job{ID: fmt.Sprintf("j%d", i)})
	}
	for range b.N {
		<-done
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}

func BenchmarkPipeline1Worker(b *testing.B)    { benchThroughput(b, 1) }
func BenchmarkPipeline8Workers(b *testing.B)   { benchThroughput(b, 8) }
func BenchmarkPipeline64Workers(b *testing.B)  { benchThroughput(b, 64) }
func BenchmarkPipeline512Workers(b *testing.B) { benchThroughput(b, 512) }

// เส้นฐาน: chan เปล่า 8 worker — ส่วนต่างคือ "ราคาของ feature"
func BenchmarkChanBaseline8Workers(b *testing.B) {
	ch := make(chan *Job, 1024)
	done := make(chan struct{}, b.N)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				done <- struct{}{}
			}
		}()
	}
	b.ResetTimer()
	for i := range b.N {
		ch <- &Job{ID: fmt.Sprintf("j%d", i)}
	}
	for range b.N {
		<-done
	}
	b.StopTimer()
	close(ch)
	wg.Wait()
}

// Enqueue ล้วนตอนไม่มี waiter — วัดราคา heap + map + broadcast อย่างเดียว
func BenchmarkEnqueueOnly(b *testing.B) {
	q := NewMemQueue(b.N+1, 3, time.Minute)
	b.ResetTimer()
	for i := range b.N {
		q.Enqueue(&Job{ID: fmt.Sprintf("j%d", i)})
	}
}

// Extend คือ heartbeat ของงานยาว — ถูกเรียกทุก visibility/3 ต่อหนึ่งงานที่ทำอยู่
// จึงเป็น path ที่ถี่ที่สุดของ workload แบบงานยาว
//
// ตัวเลขนี้มีอยู่เพื่อกันการถอยหลัง: เคยมีเวอร์ชันที่ Extend เรียก broadcastLocked()
// ทุกครั้ง ซึ่งถูกต้องเชิงตรรกะแต่ปลุก waiter ทุกตัวทุก heartbeat
// เทสทั้งชุดเขียวสนิททั้งก่อนและหลัง — มีแต่ benchmark นี้ที่เห็น
func benchExtendHeartbeat(b *testing.B, waiters int) {
	b.Helper()
	const inflight = 64
	q := NewMemQueue(10000, 5, time.Hour)
	ids := make([]string, inflight)
	for i := range inflight {
		ids[i] = fmt.Sprintf("long%d", i)
		q.Enqueue(&Job{ID: ids[i]})
		q.Dequeue(context.Background())
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range waiters { // worker ที่บล็อกรองานอยู่ — broadcast หนึ่งครั้งปลุกทั้งหมดนี้
		wg.Add(1)
		go func() { defer wg.Done(); q.Dequeue(ctx) }()
	}
	time.Sleep(50 * time.Millisecond) // ให้ waiter บล็อกจริงก่อนเริ่มจับเวลา

	b.ResetTimer()
	for i := range b.N {
		q.Extend(ids[i%inflight], time.Hour) // heartbeat = ต่อ lease ให้ยาวขึ้น
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}

func BenchmarkExtendHeartbeat0Waiters(b *testing.B)   { benchExtendHeartbeat(b, 0) }
func BenchmarkExtendHeartbeat64Waiters(b *testing.B)  { benchExtendHeartbeat(b, 64) }
func BenchmarkExtendHeartbeat512Waiters(b *testing.B) { benchExtendHeartbeat(b, 512) }
