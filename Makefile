# เกณฑ์คุณภาพของ repo นี้ — ตัวเลขทั้งสามตัวเป็น "ประตู" ไม่ใช่ "รายงาน"
# CI รัน `make ci` ตัวเดียว; แก้เกณฑ์ที่นี่ที่เดียว
COVER_MIN   ?= 100   # % ของ statement ในแพ็กเกจคิว
MUTATION_MIN ?= 100  # % ของ mutant ที่เทสต้องฆ่าได้ (ตัวที่พิสูจน์แล้วว่า equivalent อยู่ใน mutation-allow.txt)
FLAKY_RUNS  ?= 20    # จำนวนรอบที่ต้องเขียวติดกันถึงจะเชื่อว่าไม่ flaky
FUZZTIME    ?= 30s

GO ?= go

.PHONY: ci test race cover mutate fuzz flaky lint bench clean help

## ci: ทุกประตูที่ต้องผ่านก่อน merge
ci: lint race cover mutate

## test: เทสเร็ว ๆ ตอนเขียนโค้ด
test:
	$(GO) test -count=1 ./...

## race: -race ไม่ใช่ทางเลือกสำหรับโค้ดคิว — บั๊กของคิวคือบั๊กเรื่องลำดับ
race:
	$(GO) test -race -count=1 ./...

## cover: statement coverage + ประตูขั้นต่ำ — **ทั้ง repo** รวม tools/
## เครื่องมือที่รับรองคุณภาพของคนอื่น ต้องถูกวัดด้วยไม้บรรทัดเดียวกัน ไม่ใช่ยกเว้นตัวเอง
cover:
	@$(GO) test -covermode=atomic -coverprofile=coverage.out -count=1 ./... >/dev/null
	@$(GO) tool cover -func=coverage.out | grep -v '100.0%$$' || true
	@$(GO) tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '/^total:/ { \
		gsub(/%/,"",$$3); \
		printf "coverage %.1f%% (ขั้นต่ำ %s%%)\n", $$3, min; \
		if ($$3+0 < min+0) { print "❌ coverage ต่ำกว่าเกณฑ์"; exit 1 } }'

## mutate: เทสจับบั๊กได้จริงไหม — coverage สูงแต่ mutation ต่ำ = assert ที่ไม่เคยล้ม
mutate:
	@$(GO) run ./tools/mutate -pkg . -threshold $(MUTATION_MIN)

## fuzz: ให้ fuzzer หาลำดับ operation ที่ทำ invariant พัง (corpus อยู่ใน testdata/)
fuzz:
	$(GO) test -run=XXX -fuzz=FuzzQueueOps -fuzztime=$(FUZZTIME) .

## flaky: เทสที่ผ่านบ้างไม่ผ่านบ้างแย่กว่าเทสที่ไม่มี — ต้องเขียวติดกัน N รอบ
## เฉพาะแพ็กเกจคิว: ที่นั่นคือที่เดียวที่มี goroutine แข่งกัน (tools/ รัน go test เป็น
## subprocess ทำให้ช้ามาก และไม่มีอะไรให้ flake)
flaky:
	$(GO) test -race -count=$(FLAKY_RUNS) .

## lint: ต้องไม่มี output ใด ๆ
lint:
	@test -z "$$(gofmt -l .)" || { echo "❌ gofmt:"; gofmt -l .; exit 1; }
	$(GO) vet ./...
	@# ต้องเป็น if/else ไม่ใช่ `A && B || echo` — แบบหลัง echo จะกลืน exit code ของ B
	@# ทำให้ประตูกลายเป็นรายงาน (ของจริง: staticcheck เจอ SA5011 แล้ว lint ยังเขียว)
	@if command -v staticcheck >/dev/null; then staticcheck ./...; else echo "ข้าม staticcheck (ไม่ได้ติดตั้ง)"; fi
	@if command -v govulncheck >/dev/null; then govulncheck ./...; else echo "ข้าม govulncheck (ไม่ได้ติดตั้ง)"; fi

## bench: ตัวเลขจริง ไม่ใช่ความรู้สึก (§3.5)
bench:
	$(GO) test -bench=. -benchmem -run=XXX .

clean:
	rm -f coverage.out

help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  make /'
