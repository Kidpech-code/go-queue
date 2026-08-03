package fixture

import "testing"

func TestMax(t *testing.T) {
	if Max(1, 2) != 2 {
		t.Fatal("Max ผิดตั้งแต่แรก")
	}
}
