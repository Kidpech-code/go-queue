package fixture

// Larger คืนชื่อฝั่งที่มากกว่า — เท่ากันคืน "b"
//
// จงใจไม่ใช้ max(a,b): กับ int ธรรมดา `>` กับ `>=` ให้ผลเหมือนกันเป๊ะเมื่อ a == b
// (คืนค่าเท่ากันทั้งสองทาง) จึงเป็น equivalent mutant ที่ไม่มีเทสไหนฆ่าได้
// fixture ที่ใช้ยืนยันว่า "เทสแน่นแล้ว mutant ตายหมด" ต้องไม่มี mutant แบบนั้นปน
func Larger(a, b int) string {
	if a > b {
		return "a"
	}
	return "b"
}

// ลบ n-- ทิ้ง = วนไม่จบ → ต้องถูกจับด้วย timeout และนับเป็น killed
func Countdown(n int) int {
	for n > 0 {
		n--
	}
	return n
}

// + บน string: mutant + → - คอมไพล์ไม่ผ่าน → ต้องนับเป็น invalid ไม่ใช่ killed
func Greet(name string) string { return "hi " + name }

// มี ! และค่าคงที่ bool: บังคับให้ mutant "ตัด !" และ "true ↔ false" ถูกสร้าง
// **และถูกนำไปใช้จริง** — closure ที่สร้างแล้วไม่เคยถูกเรียก คือโค้ดที่ไม่มีใครตรวจ
func NotEmpty(s string) bool {
	if !(len(s) > 0) {
		return false
	}
	return true
}
