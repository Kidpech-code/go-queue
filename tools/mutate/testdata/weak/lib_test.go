package fixture

import "testing"

// เทสที่ให้ coverage 100% แต่ไม่ assert อะไรเลย — เหตุผลทั้งหมดที่เครื่องมือนี้มีอยู่
func TestMax(t *testing.T) {
	_ = Max(1, 2)
	_ = Max(2, 1)
}
