package fixture

import "testing"

func TestAll(t *testing.T) {
	for _, c := range []struct {
		a, b int
		want string
	}{{2, 1, "a"}, {1, 2, "b"}, {3, 3, "b"}} { // 3,3 คือเคสที่แยก > ออกจาก >=
		if got := Larger(c.a, c.b); got != c.want {
			t.Fatalf("Larger(%d,%d) = %q, ต้องการ %q", c.a, c.b, got, c.want)
		}
	}
	for _, n := range []int{0, 1, 3} {
		if got := Countdown(n); got != 0 {
			t.Fatalf("Countdown(%d) = %d", n, got)
		}
	}
	if got := Greet("a"); got != "hi a" {
		t.Fatalf("Greet = %q", got)
	}
	for _, c := range []struct {
		in   string
		want bool
	}{{"", false}, {"a", true}, {"ab", true}} {
		if got := NotEmpty(c.in); got != c.want {
			t.Fatalf("NotEmpty(%q) = %v, ต้องการ %v", c.in, got, c.want)
		}
	}
}
