// Command mutate — mutation testing: ตอบคำถามที่ coverage ตอบไม่ได้
//
//	coverage ตอบ "เทสรันผ่านบรรทัดนี้ไหม"
//	mutation ตอบ "ถ้าบรรทัดนี้ผิด เทสจะจับได้ไหม"
//
// 99% coverage กับ assert ที่ไม่เคยล้มเหลว = 0% ความเชื่อมั่น. ตัวนี้แก้ผิดจริง ๆ
// ทีละจุด (>= เป็น >, ลบ heap.Fix ทิ้ง, ...) แล้วดูว่าเทสแดงไหม
//
//	killed   = เทสจับได้ ✅
//	survived = เทสไม่จับ ❌ ← รูที่ต้องอุด
//	invalid  = คอมไพล์ไม่ผ่าน (ไม่นับ — ไม่ใช่บั๊กที่เขียนได้จริง)
//
// mutation score = killed / (killed + survived)
//
// ponytail: ~200 บรรทัดด้วย go/ast ล้วน แทนการเพิ่ม dependency (gremlins/go-mutesting)
// ให้ repo ที่มีไฟล์เดียว — เพดานคือชุด operator ที่ fix ไว้; อยากได้ครบกว่านี้ค่อยย้าย
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// osExit แยกออกมาเป็นตัวแปรเพื่อให้เทสเรียก main() ได้จริง ๆ โดยไม่ฆ่าโปรเซสเทส
// (เครื่องมือที่ตรวจสอบคุณภาพของคนอื่น ต้องถูกตรวจสอบเองได้ก่อน)
var osExit = os.Exit

func main() { osExit(cli(os.Args[1:], os.Stdout)) }

// cli คืน exit code แทนที่จะเรียก os.Exit เอง — ทุกเส้นทางจึงเทสได้จากในโปรเซสเดียว
//
//	0 = ผ่าน   1 = ไม่ผ่าน/ผิดพลาด   2 = ใช้ flag ผิด
func cli(args []string, out io.Writer) int {
	fs := flag.NewFlagSet("mutate", flag.ContinueOnError)
	fs.SetOutput(out)
	var (
		pkg       = fs.String("pkg", ".", "ไดเรกทอรีของแพ็กเกจที่จะกลายพันธุ์")
		threshold = fs.Float64("threshold", 100, "mutation score ต่ำสุดที่ยอมรับ (%)")
		timeout   = fs.Duration("timeout", 60*time.Second, "เวลาสูงสุดต่อ mutant (mutant ที่ทำให้ค้าง = killed)")
		allowFile = fs.String("allow", "mutation-allow.txt", "รายการ mutant ที่พิสูจน์แล้วว่า equivalent")
		// ค่าเริ่มต้น = NumCPU เต็ม: ตัวเครื่องมือเองแทบไม่กิน CPU (นั่งรอ subprocess)
		// งานจริงอยู่ใน `go test` ลูก — เดิมใช้ NumCPU-2 ทำให้ runner 4 คอร์ของ CI
		// เหลือ worker แค่ 2 ตัว = จ่ายเวลาเป็นสองเท่าเพื่อถนอมคอร์ที่ไม่มีใครใช้
		jobs = fs.Int("jobs", max(runtime.NumCPU(), 1), "จำนวน mutant ที่รันพร้อมกัน")
		list = fs.Bool("list", false, "แสดงรายการ mutant แล้วออก ไม่รันเทส")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	srcs, err := goFiles(*pkg)
	if err != nil {
		return fail(out, err)
	}
	plan, err := planMutants(srcs)
	if err != nil {
		return fail(out, err)
	}
	if *list {
		for i, m := range plan {
			fmt.Fprintf(out, "%4d  %s\n", i, m)
		}
		return 0
	}

	allow := loadAllow(*allowFile)

	if *jobs < 1 {
		*jobs = 1 // -jobs 0 = ไม่มีใครรับงาน แล้วผู้ป้อนงานค้างตลอดกาล
	}
	// สร้าง sandbox ให้ครบทุก worker ตั้งแต่ต้น แล้วใช้ตัวแรกทำ baseline
	// (เดิม worker สร้างเองระหว่างทาง → มีสาขาความล้มเหลวกลางรันที่เทสเอื้อมไม่ถึง)
	boxes, err := newSandboxes(*pkg, *jobs)
	defer closeAll(boxes) // ปิดทั้งกรณีสำเร็จและกรณีล้มกลางคัน
	if err != nil {
		return fail(out, err)
	}

	// Ctrl-C กลางรัน: defer ไม่ทำงานเมื่อโปรเซสถูกฆ่า → sandbox ค้างเป็นขยะ
	// (วัดได้จริง: kill กลางรันทิ้งไว้ jobs × ~130KB ทุกครั้ง) เก็บกวาดเองแล้ว
	// ออกด้วย 130 ตามธรรมเนียม 128+SIGINT
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	// Stop ไม่ได้ปิด channel — ต้องปิดเองไม่งั้น goroutine เฝ้าสัญญาณค้างหนึ่งตัว
	// ต่อการเรียก cli() หนึ่งครั้ง (เทสเรียกเป็นสิบครั้ง = สิบ goroutine ค้าง)
	defer func() { signal.Stop(sig); close(sig) }()
	go func() {
		if _, ok := <-sig; ok { // ok=false = จบปกติผ่าน defer ข้างบน ไม่ต้องทำอะไร
			closeAll(boxes)
			osExit(130)
		}
	}()

	// baseline ต้องเขียวก่อน ไม่งั้น "survived" ทุกตัวคือขยะ
	if rc, baseOut := boxes[0].test(*timeout); rc != 0 {
		return fail(out, fmt.Errorf("baseline เทสไม่ผ่าน — แก้ให้เขียวก่อนค่อยวัด mutation:\n%s", baseOut))
	}

	fmt.Fprintf(out, "mutants %d · jobs %d · allow %d\n\n", len(plan), *jobs, len(allow))
	return report(out, runPlan(plan, boxes, *timeout, out), allow, *threshold)
}

func fail(out io.Writer, err error) int {
	fmt.Fprintln(out, "mutate:", err)
	return 1
}

// ─────────────────────────── แผนการกลายพันธุ์ ───────────────────────────

// mutant ระบุตำแหน่งด้วย (ไฟล์, ฟังก์ชัน, คำอธิบาย, ลำดับซ้ำ)
//
// จงใจ **ไม่** ใช้เลขบรรทัดเป็น key: เพิ่มคอมเมนต์หนึ่งบรรทัดที่หัวไฟล์ก็ทำให้
// mutation-allow.txt ทั้งไฟล์ชี้ผิดหมด (เจอมาแล้ว). ชื่อฟังก์ชันขยับตามโค้ดที่มันอธิบาย
type mutant struct {
	file  string // path
	fn    string // ฟังก์ชันที่ครอบอยู่ เช่น MemQueue.Extend
	line  int    // ไว้แสดงให้คนอ่านเท่านั้น ไม่ใช่ส่วนหนึ่งของ key
	desc  string // เช่น `>= → >`
	nth   int    // ลำดับที่ n ของ mutant ที่เหมือนกันเป๊ะในฟังก์ชันเดียวกัน
	index int    // ลำดับในการ walk — ใช้เลือก mutant ตอน re-parse
}

func (m mutant) String() string { return m.key() }

func (m mutant) key() string {
	return fmt.Sprintf("%s:%s:%s#%d", filepath.Base(m.file), m.fn, m.desc, m.nth)
}

func planMutants(srcs []string) ([]mutant, error) {
	var all []mutant
	for _, src := range srcs {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, src, nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		seen := map[string]int{}
		for i, mu := range collect(f) {
			fn := enclosingFunc(f, mu.pos)
			k := fn + ":" + mu.desc
			all = append(all, mutant{
				file: src, fn: fn, line: fset.Position(mu.pos).Line,
				desc: mu.desc, nth: seen[k], index: i,
			})
			seen[k]++
		}
	}
	return all, nil
}

// enclosingFunc คืนชื่อฟังก์ชัน/เมธอดที่ครอบ pos นี้ (closure นับเป็นของฟังก์ชันแม่)
func enclosingFunc(f *ast.File, pos token.Pos) string {
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || pos < fd.Pos() || pos > fd.End() {
			continue
		}
		if fd.Recv != nil && len(fd.Recv.List) > 0 {
			t := fd.Recv.List[0].Type
			if star, ok := t.(*ast.StarExpr); ok {
				t = star.X
			}
			if id, ok := t.(*ast.Ident); ok {
				return id.Name + "." + fd.Name.Name
			}
		}
		return fd.Name.Name
	}
	return "(top-level)"
}

// site คือจุดกลายพันธุ์หนึ่งจุดบน AST ต้นนี้; apply แก้ node ตรง ๆ แล้วค่อย print
type site struct {
	pos   token.Pos
	desc  string
	apply func()
}

// swaps คือ operator ที่สลับแล้วยังคอมไพล์ผ่านเกือบทุกบริบท
// ตัวที่คอมไพล์ไม่ผ่าน (เช่น + บน string) จะถูกนับเป็น invalid อัตโนมัติ — ไม่ต้องพึ่ง type check
var swaps = map[token.Token][]token.Token{
	token.GTR:  {token.GEQ, token.LSS},
	token.GEQ:  {token.GTR, token.LSS},
	token.LSS:  {token.LEQ, token.GTR},
	token.LEQ:  {token.LSS, token.GTR},
	token.EQL:  {token.NEQ},
	token.NEQ:  {token.EQL},
	token.LAND: {token.LOR},
	token.LOR:  {token.LAND},
	token.ADD:  {token.SUB},
	token.SUB:  {token.ADD},
	token.MUL:  {token.QUO},
	token.SHL:  {token.SHR},
}

// collect เดิน AST แบบ deterministic → index เดียวกันหมายถึง mutant ตัวเดียวกันเสมอ
// (worker แต่ละตัว parse ไฟล์เองแล้วเลือกด้วย index — ไม่แชร์ AST กัน)
func collect(f *ast.File) []site {
	var out []site
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BinaryExpr:
			for _, op := range swaps[x.Op] {
				old, nw := x.Op, op
				out = append(out, site{x.OpPos, fmt.Sprintf("%s → %s", old, nw), func() { x.Op = nw }})
			}

		case *ast.UnaryExpr: // ตัด ! ทิ้ง — จับเงื่อนไขขอบแบบ !t.After(now)
			if x.Op == token.NOT {
				out = append(out, site{x.OpPos, "ตัด !", func() { *x = ast.UnaryExpr{OpPos: x.OpPos, Op: token.ADD, X: x.X} }})
			}

		case *ast.Ident: // true ↔ false (เฉพาะที่เป็นค่าคงที่ ไม่ใช่ชื่อตัวแปร)
			if flip := map[string]string{"true": "false", "false": "true"}[x.Name]; flip != "" {
				out = append(out, site{x.Pos(), x.Name + " → " + flip, func() { x.Name = flip }})
			}

		case *ast.BasicLit: // เลข: 0↔1, n→n+1 — จับ off-by-one และ threshold ที่ hardcode
			if x.Kind == token.INT {
				if v, err := strconv.Atoi(x.Value); err == nil {
					nw := strconv.Itoa(v + 1)
					if v == 1 {
						nw = "0"
					}
					old := x.Value
					out = append(out, site{x.Pos(), old + " → " + nw, func() { x.Value = nw }})
				}
			}

		case *ast.BlockStmt: // ลบ statement ทิ้ง — จับ heap.Fix/delete/broadcast/Attempt++/j.index=… ที่หายไป
			for i, s := range x.List {
				switch t := s.(type) {
				case *ast.ExprStmt, *ast.IncDecStmt:
				case *ast.AssignStmt: // เฉพาะ = ไม่ใช่ := (ลบ := แล้วตัวแปรหาย = คอมไพล์ไม่ผ่านทุกครั้ง)
					if t.Tok != token.ASSIGN {
						continue
					}
				default:
					continue
				}
				i, old := i, s
				out = append(out, site{s.Pos(), "ลบ " + oneLine(old), func() { x.List[i] = &ast.EmptyStmt{Semicolon: old.Pos(), Implicit: true} }})
			}
		}
		return true
	})
	return out
}

func oneLine(n ast.Node) string {
	var sb strings.Builder
	_ = printer.Fprint(&sb, token.NewFileSet(), n)
	s := strings.Join(strings.Fields(sb.String()), " ")
	if len(s) > 48 {
		s = s[:45] + "..."
	}
	return s
}

// ─────────────────────────── การรัน ───────────────────────────

type result struct {
	mutant
	verdict string // killed | survived | invalid
}

func runPlan(plan []mutant, boxes []*sandbox, timeout time.Duration, out io.Writer) []result {
	results := make([]result, len(plan))
	var done, survived int
	var mu sync.Mutex
	work := make(chan int)
	var wg sync.WaitGroup

	for _, sb := range boxes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				m := plan[i]
				v := "invalid"
				if err := sb.mutate(m); err == nil {
					switch rc, _ := sb.test(timeout); rc {
					case 0:
						v = "survived"
					case 2: // build error = mutant ที่เขียนไม่ได้จริง ไม่ใช่ความผิดของเทส
						v = "invalid"
					default:
						v = "killed"
					}
				}
				sb.restore(m.file)
				results[i] = result{m, v}

				mu.Lock()
				done++
				if v == "survived" {
					survived++
				}
				fmt.Fprintf(out, "\r%d/%d  survived=%d  ", done, len(plan), survived)
				mu.Unlock()
			}
		}()
	}
	for i := range plan {
		work <- i
	}
	close(work)
	wg.Wait()
	fmt.Fprint(out, "\r\033[K")
	return results
}

// newSandboxes คืน sandbox ที่สร้างสำเร็จแล้วมาด้วยเสมอ แม้จะล้มกลางคัน
// ผู้เรียกจึงปิดทีเดียวด้วย defer ได้ ไม่ต้องมีทางเก็บกวาดสองทาง
func newSandboxes(pkg string, n int) ([]*sandbox, error) {
	boxes := make([]*sandbox, 0, n)
	for range n {
		sb, err := newSandbox(pkg)
		if err != nil {
			return boxes, err
		}
		boxes = append(boxes, sb)
	}
	return boxes, nil
}

func closeAll(boxes []*sandbox) {
	for _, sb := range boxes {
		sb.close()
	}
}

// sandbox = สำเนาแพ็กเกจใน temp dir — worker แต่ละตัวมีของตัวเอง จึงกลายพันธุ์พร้อมกันได้
type sandbox struct {
	dir  string
	orig map[string][]byte // ชื่อไฟล์ → เนื้อไฟล์เดิม
}

func newSandbox(pkg string) (*sandbox, error) {
	entries, err := os.ReadDir(pkg)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "mutate-")
	if err != nil {
		return nil, err
	}
	sb := &sandbox{dir: dir, orig: map[string][]byte{}}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !(strings.HasSuffix(n, ".go") || n == "go.mod" || n == "go.sum") {
			continue
		}
		// รวม read+write เป็น error เดียว: `if err != nil { return err }` สองชั้น
		// สร้างสาขาที่เทสเอื้อมไม่ถึง (write ลง temp dir ที่เพิ่งสร้างเองแทบไม่มีวันล้ม)
		b, err := os.ReadFile(filepath.Join(pkg, n))
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, n), b, 0o644)
		}
		if err != nil {
			sb.close()
			return nil, err
		}
		sb.orig[n] = b
	}
	return sb, nil
}

func (s *sandbox) close() { os.RemoveAll(s.dir) }

func (s *sandbox) restore(src string) {
	if b, ok := s.orig[filepath.Base(src)]; ok {
		os.WriteFile(filepath.Join(s.dir, filepath.Base(src)), b, 0o644)
	}
}

func (s *sandbox) mutate(m mutant) error {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, m.file, nil, parser.ParseComments)
	if err != nil {
		return err
	}
	sites := collect(f)
	if m.index >= len(sites) {
		return fmt.Errorf("mutant %s หายไปจาก AST — โค้ดถูกแก้ระหว่างรัน", m.key())
	}
	sites[m.index].apply()

	// print ลง buffer ก่อน: เขียนลงหน่วยความจำไม่มีทางล้ม (AST เพิ่ง parse มาเอง)
	// จึงเหลือ error เดียวคือตอนเขียนไฟล์ ซึ่งคืนตรง ๆ ได้โดยไม่ต้องมีสาขาที่เทสไม่ถึง
	var buf bytes.Buffer
	_ = printer.Fprint(&buf, fset, f)
	return os.WriteFile(filepath.Join(s.dir, filepath.Base(m.file)), buf.Bytes(), 0o644)
}

// test คืน exit code: 0 = ผ่าน, 2 = คอมไพล์ไม่ผ่าน, อื่น ๆ = เทสแดง
// mutant ที่ทำให้ค้าง (เช่น ลบ n-- ทิ้งจนวนไม่จบ) นับเป็น killed — เทสจับได้ด้วยการ timeout
//
// -failfast: mutant ถูก "ฆ่า" เมื่อมีเทสแดง ≥ 1 ตัว — เทสที่เหลือไม่เปลี่ยนคำตัดสิน
// จึงหยุดได้ทันทีที่เจอตัวแรก (86% ของ mutant ตาย → ประหยัดเกือบทั้งชุดต่อ mutant)
// survivor ไม่มีเทสแดงให้หยุด = รันเต็มชุดเหมือนเดิมเป๊ะ ⇒ คำตัดสินเหมือนเดิมทุกตัว
func (s *sandbox) test(timeout time.Duration) (int, string) {
	cmd := exec.Command("go", "test", "-count=1", "-failfast", "-timeout", timeout.String(), ".")
	cmd.Dir = s.dir
	cmd.Env = append(os.Environ(), "GOFLAGS=") // กัน -race หลุดมาจาก env — mutation ต้องเร็ว
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if ee.ExitCode() == 1 && strings.Contains(string(out), "[build failed]") {
			return 2, string(out)
		}
		return ee.ExitCode(), string(out)
	}
	return 1, string(out) // เรียก go ไม่ได้เลย (ไม่อยู่ใน PATH) — ไม่ใช่ผลของ mutant
}

// ─────────────────────────── รายงาน ───────────────────────────

func report(out io.Writer, results []result, allow map[string]bool, threshold float64) int {
	var killed, survived, invalid, allowed int
	var alive []result
	for _, r := range results {
		switch r.verdict {
		case "killed":
			killed++
		case "invalid":
			invalid++
		case "survived":
			if allow[r.key()] {
				allowed++
				continue
			}
			survived++
			alive = append(alive, r)
		}
	}

	if len(alive) > 0 {
		sort.Slice(alive, func(i, j int) bool {
			if alive[i].file != alive[j].file {
				return alive[i].file < alive[j].file
			}
			return alive[i].line < alive[j].line
		})
		fmt.Fprintln(out, "MUTANT ที่รอดชีวิต — เทสไม่จับ:")
		for _, r := range alive {
			fmt.Fprintf(out, "  %s:%d\t%s\n", r.file, r.line, r.desc)
		}
		fmt.Fprintln(out, "\nอุดด้วยเทสใหม่ หรือถ้าพิสูจน์ได้ว่า equivalent ให้ใส่ mutation-allow.txt:")
		for _, r := range alive {
			fmt.Fprintf(out, "  %s\n", r.key())
		}
		fmt.Fprintln(out)
	}

	// entry ที่ไม่ตรงกับ mutant ตัวไหนเลย = ข้อยกเว้นที่หมดอายุแล้ว (โค้ดถูกแก้/เปลี่ยนชื่อ)
	// ไม่อันตรายต่อความถูกต้อง — มันยกเว้น "อะไรก็ไม่รู้" — แต่ทำให้คนอ่านไฟล์เข้าใจผิดว่า
	// ยังมีเหตุผลนั้นคุ้มครองอยู่ ซึ่งเป็นวิธีที่ allowlist เน่าโดยไม่มีใครสังเกต
	if stale := staleAllow(results, allow); len(stale) > 0 {
		fmt.Fprintln(out, "⚠️  mutation-allow.txt มี entry ที่ไม่ตรงกับ mutant ตัวไหนเลย — ลบทิ้งได้:")
		for _, k := range stale {
			fmt.Fprintf(out, "  %s\n", k)
		}
		fmt.Fprintln(out)
	}

	total := killed + survived
	score := 100.0
	if total > 0 {
		score = float64(killed) / float64(total) * 100
	}
	fmt.Fprintf(out, "killed %d · survived %d · equivalent %d · invalid %d\nmutation score %.1f%% (ขั้นต่ำ %.1f%%)\n",
		killed, survived, allowed, invalid, score, threshold)
	if score < threshold {
		return 1
	}
	return 0
}

func staleAllow(results []result, allow map[string]bool) []string {
	live := make(map[string]bool, len(results))
	for _, r := range results {
		live[r.key()] = true
	}
	var stale []string
	for k := range allow {
		if !live[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(stale)
	return stale
}

// loadAllow อ่านรายการ equivalent mutant. อ่านไม่ได้ = ถือว่าไม่มีข้อยกเว้น
// (ตั้งใจให้เงียบ: repo ที่ยังไม่มีข้อยกเว้นไม่ควรต้องสร้างไฟล์เปล่าไว้ให้เกะกะ)
func loadAllow(path string) map[string]bool {
	m := map[string]bool{}
	b, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	// บรรทัดที่ขึ้นต้นด้วย # คือคอมเมนต์; ที่เหลือคือ key ทั้งบรรทัด
	// (ตัดที่ # กลางบรรทัดไม่ได้ เพราะตัว key เองลงท้ายด้วย #n)
	// ตัด BOM ทิ้ง และ TrimSpace กิน \r ให้ → ไฟล์ที่แก้บน Windows ใช้ได้เหมือนกัน
	for line := range strings.SplitSeq(strings.TrimPrefix(string(b), "\ufeff"), "\n") {
		if k := strings.TrimSpace(line); k != "" && !strings.HasPrefix(k, "#") {
			m[k] = true
		}
	}
	return m
}

func goFiles(dir string) ([]string, error) {
	all, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, f := range all {
		if !strings.HasSuffix(f, "_test.go") {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ไม่พบไฟล์ .go ใน %s", dir)
	}
	return out, nil
}
