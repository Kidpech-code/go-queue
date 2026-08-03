package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// เครื่องมือที่รับรองคุณภาพของคนอื่น ต้องถูกตรวจสอบเองก่อน
//
// ถ้า classification ของ mutant ผิด (นับ build failure เป็น killed) mutation score
// จะพองขึ้นโดยไม่มีใครรู้ — และตัวเลข 100% ทั้งหมดใน README จะเป็นเรื่องโกหก
// เทสในไฟล์นี้จึงเน้นที่ "การตัดสิน" เป็นหลัก ไม่ใช่แค่ว่าโปรแกรมรันผ่าน
//
// fixture อยู่ใน testdata/ (go tool ข้ามให้อัตโนมัติ) แต่ละตัวมี go.mod ของตัวเอง
// เพราะ sandbox คัดลอกทั้งแพ็กเกจไปรันจริง

func fixture(t *testing.T, name string) string {
	t.Helper()
	// คัดลอกไป temp dir: เทสบางตัวแก้ไฟล์ ห้ามแตะ testdata ตัวจริง
	dst := t.TempDir()
	src := filepath.Join("testdata", name)
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("ReadDir(%s) = %v", src, err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// ── end-to-end: การตัดสินต้องถูก ────────────────────────────────────

// fixture "strong" มีเทสแน่น → mutant ต้องตายหมด และต้องมี invalid อย่างน้อยหนึ่งตัว
// (mutant `+` → `-` บน string คอมไพล์ไม่ผ่าน — ห้ามนับเป็น killed ไม่งั้น score พองขึ้นฟรี ๆ)
func TestCLIStrongFixtureScores100(t *testing.T) {
	var out bytes.Buffer
	// timeout สั้น: fixture มี mutant ที่ลบ n-- ทิ้งแล้ววนไม่จบ — ต้องถูกนับเป็น killed
	code := cli([]string{"-pkg", fixture(t, "strong"), "-timeout", "20s", "-allow", "/dev/null"}, &out)
	s := out.String()
	if code != 0 {
		t.Fatalf("exit = %d, ต้องการ 0\n%s", code, s)
	}
	if !strings.Contains(s, "survived 0 ") {
		t.Errorf("ต้องไม่มี mutant รอด:\n%s", s)
	}
	if !strings.Contains(s, "mutation score 100.0%") {
		t.Errorf("score ต้องเป็น 100.0%%:\n%s", s)
	}
	// ★ ถ้า invalid = 0 แปลว่า build failure ถูกนับเป็น killed → score โกหก
	if strings.Contains(s, "invalid 0\n") {
		t.Errorf("mutant ที่คอมไพล์ไม่ผ่านต้องถูกนับเป็น invalid ไม่ใช่ killed:\n%s", s)
	}
}

// fixture "weak" มีเทสที่ให้ coverage 100% แต่ไม่ assert อะไร → mutant ต้องรอด
// นี่คือเหตุผลทั้งหมดที่เครื่องมือนี้มีอยู่ ถ้าเทสนี้ผ่านตอน score = 100% แปลว่าพัง
func TestCLIWeakFixtureReportsSurvivors(t *testing.T) {
	var out bytes.Buffer
	code := cli([]string{"-pkg", fixture(t, "weak"), "-timeout", "20s", "-allow", "/dev/null"}, &out)
	s := out.String()
	if code != 1 {
		t.Fatalf("exit = %d, ต้องการ 1 (score ต่ำกว่าเกณฑ์)\n%s", code, s)
	}
	if !strings.Contains(s, "MUTANT ที่รอดชีวิต") {
		t.Errorf("ต้องรายงาน mutant ที่รอด:\n%s", s)
	}
	if strings.Contains(s, "mutation score 100.0%") {
		t.Errorf("เทสที่ไม่ assert อะไรเลย ต้องไม่ได้ 100%%:\n%s", s)
	}
}

// allowlist ต้องยกเว้น mutant ที่ระบุไว้ ทำให้ผ่านเกณฑ์ได้
func TestCLIAllowlistLetsSurvivorsPass(t *testing.T) {
	pkg := fixture(t, "weak")
	var list bytes.Buffer
	if code := cli([]string{"-pkg", pkg, "-list"}, &list); code != 0 {
		t.Fatalf("-list exit = %d", code)
	}
	// เก็บ key ของ mutant ทุกตัวมาใส่ allowlist → ไม่เหลืออะไรให้ตก
	var keys []string
	for line := range strings.SplitSeq(strings.TrimSpace(list.String()), "\n") {
		if _, k, ok := strings.Cut(strings.TrimSpace(line), "  "); ok {
			keys = append(keys, strings.TrimSpace(k))
		}
	}
	if len(keys) == 0 {
		t.Fatalf("-list ไม่คืน mutant เลย:\n%s", list.String())
	}
	allow := filepath.Join(t.TempDir(), "allow.txt")
	body := "# เหตุผล\n\n" + strings.Join(keys, "\n") + "\n"
	if err := os.WriteFile(allow, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	code := cli([]string{"-pkg", pkg, "-timeout", "20s", "-allow", allow}, &out)
	if s := out.String(); code != 0 || !strings.Contains(s, "mutation score 100.0%") {
		t.Errorf("exit = %d — allowlist ต้องทำให้ผ่านเกณฑ์ได้\n%s", code, s)
	}
}

// baseline แดง = ตัวเลขทุกตัวหลังจากนั้นไร้ความหมาย ต้องหยุดทันที
func TestCLIRejectsRedBaseline(t *testing.T) {
	var out bytes.Buffer
	code := cli([]string{"-pkg", fixture(t, "broken"), "-timeout", "20s", "-allow", "/dev/null"}, &out)
	if code != 1 || !strings.Contains(out.String(), "baseline เทสไม่ผ่าน") {
		t.Errorf("exit = %d — baseline แดงต้องหยุดก่อนวัดอะไรทั้งสิ้น\n%s", code, out.String())
	}
}

func TestCLIListDoesNotRunTests(t *testing.T) {
	var out bytes.Buffer
	// baseline แดง แต่ -list ไม่รันเทส จึงต้องสำเร็จ
	if code := cli([]string{"-pkg", fixture(t, "broken"), "-list"}, &out); code != 0 {
		t.Fatalf("exit = %d\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "lib.go:Max:") {
		t.Errorf("-list ต้องแสดง key ของ mutant:\n%s", out.String())
	}
}

func TestCLIErrors(t *testing.T) {
	for name, c := range map[string]struct {
		args []string
		want int
		msg  string
	}{
		"flag ไม่รู้จัก":     {[]string{"-ไม่มีจริง"}, 2, ""},
		"ไม่มีไฟล์ .go":      {[]string{"-pkg", t.TempDir()}, 1, "ไม่พบไฟล์ .go"},
		"ไดเรกทอรีไม่มีจริง": {[]string{"-pkg", "/ไม่มีจริง/xyz"}, 1, "ไม่พบไฟล์ .go"},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if code := cli(c.args, &out); code != c.want {
				t.Errorf("exit = %d, ต้องการ %d\n%s", code, c.want, out.String())
			}
			if !strings.Contains(out.String(), c.msg) {
				t.Errorf("output = %q, ต้องมี %q", out.String(), c.msg)
			}
		})
	}
}

func TestCLIRejectsUnparsableSource(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte("package x\nfunc ("), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := cli([]string{"-pkg", dir}, &out); code != 1 {
		t.Errorf("exit = %d, ต้องการ 1 (parse ไม่ได้)\n%s", code, out.String())
	}
}

// TMPDIR พังจริง ๆ ในเครื่องที่พื้นที่เต็ม — ต้องรายงาน ไม่ใช่ panic
func TestCLIReportsSandboxFailure(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "ไม่มีจริง"))
	var out bytes.Buffer
	if code := cli([]string{"-pkg", fixture(t, "strong")}, &out); code != 1 {
		t.Errorf("exit = %d, ต้องการ 1\n%s", code, out.String())
	}
}

// main() ต้องส่ง exit code ของ cli ออกไปตรง ๆ
func TestMainPropagatesExitCode(t *testing.T) {
	oldExit, oldArgs := osExit, os.Args
	t.Cleanup(func() { osExit, os.Args = oldExit, oldArgs })

	got := -1
	osExit = func(code int) { got = code }
	os.Args = []string{"mutate", "-pkg", t.TempDir()} // ไม่มีไฟล์ .go → 1
	main()
	if got != 1 {
		t.Errorf("exit code = %d, ต้องการ 1", got)
	}
}

// ── หน่วยย่อย: จุดที่ตัดสินผิดแล้วตัวเลขพองขึ้นเงียบ ๆ ──────────────────

func TestSandboxTestClassifiesOutcomes(t *testing.T) {
	pkg := fixture(t, "strong")
	sb, err := newSandbox(pkg)
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()

	if rc, out := sb.test(30 * time.Second); rc != 0 {
		t.Fatalf("baseline rc = %d\n%s", rc, out)
	}

	// คอมไพล์ไม่ผ่าน → 2 (invalid) ไม่ใช่ 1 (killed)
	if err := os.WriteFile(filepath.Join(sb.dir, "lib.go"), []byte("package fixture\nfunc Max("), 0o644); err != nil {
		t.Fatal(err)
	}
	if rc, out := sb.test(30 * time.Second); rc != 2 {
		t.Errorf("build ไม่ผ่าน rc = %d, ต้องการ 2 — ไม่งั้น mutant ที่เขียนไม่ได้จริงจะถูกนับเป็น killed\n%s", rc, out)
	}

	// เทสแดง → ไม่ใช่ 0 และไม่ใช่ 2
	//
	// แก้ค่าที่คืนออกไป แทนการเขียนไฟล์ใหม่ทั้งไฟล์: เขียนใหม่แล้วลืม signature ตัวใดตัวหนึ่ง
	// จะกลายเป็น build failure (rc=2) แทนที่จะเป็นเทสแดง แล้วเทสนี้จะพังทุกครั้งที่ fixture โต
	// (พลาดมาแล้วสองรอบ — เครื่องมือตัดสินถูก เทสตัวนี้ต่างหากที่ผิด)
	sb.restore("lib.go")
	src := sb.orig["lib.go"]
	broken := bytes.ReplaceAll(src, []byte(`return "a"`), []byte(`return "z"`))
	if bytes.Equal(src, broken) {
		t.Fatal(`fixture ไม่มี return "a" ให้แก้แล้ว — เทสนี้กลายเป็นของว่าง`)
	}
	if err := os.WriteFile(filepath.Join(sb.dir, "lib.go"), broken, 0o644); err != nil {
		t.Fatal(err)
	}
	if rc, _ := sb.test(30 * time.Second); rc == 0 || rc == 2 {
		t.Errorf("เทสแดง rc = %d, ต้องเป็น killed", rc)
	}

	// restore ต้องคืนไฟล์เดิมได้จริง และไฟล์ที่ไม่รู้จักต้องไม่ทำอะไรพัง
	sb.restore("lib.go")
	sb.restore("ไม่เคยคัดลอกมา.go")
	if rc, out := sb.test(30 * time.Second); rc != 0 {
		t.Errorf("หลัง restore rc = %d, ต้องกลับมาเขียว\n%s", rc, out)
	}
}

// เรียก go ไม่ได้เลย ≠ mutant ถูกฆ่า — ต้องไม่นับเป็นความสำเร็จของเทส
func TestSandboxTestWhenGoMissing(t *testing.T) {
	sb, err := newSandbox(fixture(t, "strong"))
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	t.Setenv("PATH", "")
	if rc, _ := sb.test(5 * time.Second); rc == 0 {
		t.Error("หา go ไม่เจอต้องไม่ถูกนับเป็น survived")
	}
}

func TestSandboxErrors(t *testing.T) {
	if _, err := newSandbox(filepath.Join(t.TempDir(), "ไม่มีจริง")); err == nil {
		t.Error("ไดเรกทอรีไม่มีจริงต้อง error")
	}

	// อ่านไฟล์ต้นทางไม่ได้ (เช่นสิทธิ์) ต้องรายงาน ไม่ใช่คัดลอกไฟล์เปล่าไปเงียบ ๆ
	dir := t.TempDir()
	locked := filepath.Join(dir, "a.go")
	if err := os.WriteFile(locked, []byte("package x"), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := newSandbox(dir); err == nil {
		t.Error("อ่านไฟล์ไม่ได้ต้อง error")
	}
	os.Chmod(locked, 0o644)
}

func TestSandboxMutateErrors(t *testing.T) {
	pkg := fixture(t, "strong")
	sb, err := newSandbox(pkg)
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()

	src := filepath.Join(pkg, "lib.go")
	if err := sb.mutate(mutant{file: src, index: 99999}); err == nil {
		t.Error("index เกินจำนวน mutant ต้อง error ไม่ใช่ panic")
	}

	bad := filepath.Join(t.TempDir(), "bad.go")
	if err := os.WriteFile(bad, []byte("package x\nfunc ("), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sb.mutate(mutant{file: bad}); err == nil {
		t.Error("ไฟล์ที่ parse ไม่ได้ต้อง error")
	}
}

// สร้าง sandbox ไม่สำเร็จ ต้องคืนตัวที่สร้างไปแล้วมาให้ปิดด้วย ไม่ใช่ปล่อยรั่ว
func TestNewSandboxesReturnsPartialOnFailure(t *testing.T) {
	boxes, err := newSandboxes(filepath.Join(t.TempDir(), "ไม่มีจริง"), 3)
	if err == nil {
		t.Fatal("ไดเรกทอรีไม่มีจริงต้อง error")
	}
	closeAll(boxes) // ต้องไม่ panic แม้จะว่าง

	boxes, err = newSandboxes(fixture(t, "strong"), 2)
	if err != nil || len(boxes) != 2 {
		t.Fatalf("newSandboxes = %d ตัว, %v", len(boxes), err)
	}
	dirs := []string{boxes[0].dir, boxes[1].dir}
	if dirs[0] == dirs[1] {
		t.Error("worker ต้องมี sandbox คนละตัว ไม่งั้นกลายพันธุ์ทับกัน")
	}
	closeAll(boxes)
	for _, d := range dirs {
		if _, err := os.Stat(d); err == nil {
			t.Errorf("closeAll ไม่ได้ลบ %s — temp dir รั่วทุกครั้งที่รัน", d)
		}
	}
}

// -jobs 0 เคยทำให้ค้างตลอดกาล: ไม่มี worker แต่ยังป้อนงานเข้า channel
func TestCLIClampsJobs(t *testing.T) {
	var out bytes.Buffer
	code := cli([]string{"-pkg", fixture(t, "strong"), "-jobs", "0", "-timeout", "20s", "-allow", "/dev/null"}, &out)
	if code != 0 {
		t.Errorf("exit = %d, ต้องการ 0 (jobs ต้องถูกดันขึ้นเป็น 1 ไม่ใช่ค้าง)\n%s", code, out.String())
	}
}

// ── AST: ตัวที่ผิดแล้วรายงานชี้ผิดจุด ────────────────────────────────

const astSrc = `package p

var Top = 5

type T struct{}

func (t T) Value() bool { return true }

func (t *T) Ptr() bool { return false }

func Plain(a, b int) int {
	x := 0
	x = a + b
	if !(a > b) && a != 0 || b <= 1 {
		x++
	}
	println(x)
	s := "ยาวมากจนต้องถูกตัดให้สั้นลงเพื่อให้รายงานอ่านง่าย"
	_ = s + "ต่อท้ายให้ยาวขึ้นอีกเยอะ ๆ จนเกินสี่สิบแปดตัวอักษรแน่นอน"
	return x * 2
}

func Hex() int { return 0xFF }
`

func TestCollectAndEnclosingFunc(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "p.go", astSrc, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	sites := collect(f)

	byDesc := map[string]int{}
	fns := map[string]bool{}
	for _, s := range sites {
		byDesc[s.desc]++
		fns[enclosingFunc(f, s.pos)] = true
	}

	// operator ทุกกลุ่มต้องถูกเก็บ ไม่งั้นบั๊กทั้งชั้นจะมองไม่เห็น
	for _, want := range []string{"> → >=", "!= → ==", "|| → &&", "&& → ||", "<= → <", "+ → -", "* → /", "ตัด !", "true → false", "false → true"} {
		if byDesc[want] == 0 {
			t.Errorf("ไม่มี mutant %q", want)
		}
	}
	// 1 → 0 (ไม่ใช่ 1 → 2) และ 5 → 6
	if byDesc["1 → 0"] == 0 || byDesc["5 → 6"] == 0 || byDesc["0 → 1"] == 0 {
		t.Errorf("การกลายพันธุ์ของเลขผิด: %v", byDesc)
	}
	// 0xFF: Atoi ไม่ผ่าน → ต้องข้าม ไม่ใช่ panic
	if byDesc["0xFF → 256"] != 0 {
		t.Error("เลขฐานสิบหกต้องถูกข้าม")
	}
	// ลบ statement: x++ (IncDec), x = a+b (assign =), println(x) (expr)
	// แต่ x := 0 และ s := ... (define) ต้องไม่ถูกลบ เพราะจะคอมไพล์ไม่ผ่านทุกครั้ง
	var deletes []string
	for d := range byDesc {
		if strings.HasPrefix(d, "ลบ ") {
			deletes = append(deletes, d)
		}
	}
	if len(deletes) < 3 {
		t.Errorf("statement ที่ลบได้ = %v, ต้องมีอย่างน้อย 3", deletes)
	}
	for _, d := range deletes {
		if strings.Contains(d, ":=") {
			t.Errorf("ห้ามสร้าง mutant ที่ลบ := (%q) — คอมไพล์ไม่ผ่านทุกครั้ง = สิ้นเปลืองเปล่า", d)
		}
		if len(d) > 60 {
			t.Errorf("คำอธิบายยาวเกินไป ไม่ถูกตัด: %q", d)
		}
	}

	// ชื่อฟังก์ชันในรายงานต้องแยก method จาก function และแยก pointer จาก value receiver
	for _, want := range []string{"Plain", "T.Value", "T.Ptr", "(top-level)"} {
		if !fns[want] {
			t.Errorf("enclosingFunc ไม่คืน %q (ได้ %v)", want, fns)
		}
	}
}

func TestOneLineTruncates(t *testing.T) {
	fset := token.NewFileSet()
	f, _ := parser.ParseFile(fset, "p.go", astSrc, parser.ParseComments)
	for _, s := range collect(f) {
		if len(s.desc) > 64 {
			t.Errorf("desc ยาว %d: %q", len(s.desc), s.desc)
		}
	}
}

func TestKeyIsStableAcrossLineShifts(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) []mutant {
		t.Helper()
		p := filepath.Join(dir, "a.go")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		plan, err := planMutants([]string{p})
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	before := write("package p\n\nfunc F(a, b int) bool { return a > b }\n")
	// แทรกคอมเมนต์และฟังก์ชันใหม่ข้างบน → เลขบรรทัดขยับหมด แต่ key ต้องเหมือนเดิม
	after := write("package p\n\n// คอมเมนต์ใหม่\n// อีกบรรทัด\nfunc G() int { return 7 }\n\nfunc F(a, b int) bool { return a > b }\n")

	find := func(plan []mutant, key string) bool {
		for _, m := range plan {
			if m.key() == key {
				return true
			}
		}
		return false
	}
	k := "a.go:F:> → >=#0"
	if !find(before, k) {
		t.Fatalf("ไม่เจอ %q ใน %v", k, before)
	}
	if !find(after, k) {
		t.Errorf("key เปลี่ยนไปเมื่อเลขบรรทัดขยับ — mutation-allow.txt ทั้งไฟล์จะชี้ผิด: %v", after)
	}
	if before[0].line == after[len(after)-1].line {
		t.Error("เทสนี้ไม่ได้ทำให้บรรทัดขยับจริง")
	}
}

// ── allowlist ────────────────────────────────────────────────────────

func TestLoadAllow(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "allow.txt")
	// BOM + CRLF + คอมเมนต์ + บรรทัดว่าง + ช่องว่างท้ายบรรทัด
	body := "\ufeff# หัวเรื่อง\r\n\r\n  a.go:F:> → >=#0  \r\nb.go:G:ลบ x++#1\n# ท้ายไฟล์\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := loadAllow(p)
	want := []string{"a.go:F:> → >=#0", "b.go:G:ลบ x++#1"}
	if len(got) != len(want) {
		t.Fatalf("อ่านได้ %v, ต้องการ %v", got, want)
	}
	for _, k := range want {
		if !got[k] {
			t.Errorf("ไม่เจอ key %q — entry ที่ match ไม่ติดจะทำให้ประตูแดงโดยไม่มีเหตุผล", k)
		}
	}
	if len(loadAllow(filepath.Join(dir, "ไม่มีไฟล์"))) != 0 {
		t.Error("ไม่มีไฟล์ = ไม่มีข้อยกเว้น ไม่ใช่ error")
	}
}

func TestReportFlagsStaleAllowEntries(t *testing.T) {
	results := []result{
		{mutant{file: "a.go", fn: "F", desc: "> → >=", line: 3}, "killed"},
		{mutant{file: "a.go", fn: "F", desc: "< → <=", line: 4}, "survived"},
	}
	allow := map[string]bool{
		"a.go:F:< → <=#0":    true, // ตรงกับของจริง
		"a.go:หายไปแล้ว:x#0": true, // ค้างจากโค้ดเวอร์ชันเก่า
	}
	var out bytes.Buffer
	if code := report(&out, results, allow, 100); code != 0 {
		t.Fatalf("exit = %d — allowlist ควรทำให้ผ่าน\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "หายไปแล้ว") {
		t.Errorf("ต้องเตือน entry ที่ไม่ตรงกับ mutant ตัวไหนเลย:\n%s", out.String())
	}
	if strings.Contains(out.String(), "a.go:F:< → <=#0\n  a.go") {
		t.Error("entry ที่ยังใช้งานอยู่ต้องไม่ถูกเตือน")
	}
}

func TestReportScoreAndOrdering(t *testing.T) {
	// invalid ล้วน → ไม่มีตัวหาร ต้องไม่หารศูนย์ และต้องนับเป็นผ่าน
	var out bytes.Buffer
	if code := report(&out, []result{{mutant{file: "a.go"}, "invalid"}}, nil, 100); code != 0 {
		t.Errorf("invalid ล้วน exit = %d\n%s", code, out.String())
	}

	// เรียงรายงานตามไฟล์แล้วบรรทัด — รายงานที่เรียงมั่วอ่านไม่ออกตอนมี 40 ตัว
	out.Reset()
	results := []result{
		{mutant{file: "b.go", line: 1, desc: "x"}, "survived"},
		{mutant{file: "a.go", line: 9, desc: "y"}, "survived"},
		{mutant{file: "a.go", line: 2, desc: "z"}, "survived"},
	}
	if code := report(&out, results, nil, 100); code != 1 {
		t.Errorf("มี survivor แต่ exit = %d", code)
	}
	s := out.String()
	ia, ib, ic := strings.Index(s, "a.go:2"), strings.Index(s, "a.go:9"), strings.Index(s, "b.go:1")
	if !(ia < ib && ib < ic) {
		t.Errorf("ลำดับรายงานผิด (a:2 < a:9 < b:1):\n%s", s)
	}

	// ต่ำกว่าเกณฑ์ = แดง; ตั้งเกณฑ์ต่ำลง = เขียว
	out.Reset()
	if code := report(&out, []result{{mutant{}, "killed"}, {mutant{}, "survived"}}, nil, 40); code != 0 {
		t.Errorf("score 50%% กับเกณฑ์ 40%% ต้องผ่าน:\n%s", out.String())
	}
}

func TestGoFilesSkipsTestFilesAndBadPatterns(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"a.go", "b_test.go", "c.txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("package p"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := goFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || filepath.Base(got[0]) != "a.go" {
		t.Errorf("goFiles = %v, ต้องการแค่ a.go (ห้ามกลายพันธุ์ตัวเทสเอง)", got)
	}

	// ชื่อไดเรกทอรีที่มี [ ค้าง ทำให้ glob pattern เสีย — ต้องคืน error ไม่ใช่เงียบ
	if _, err := goFiles("ก[ข"); err == nil {
		t.Error("pattern เสียต้อง error")
	}
}
