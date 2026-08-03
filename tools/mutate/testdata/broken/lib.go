package fixture

// บั๊กจงใจ: ต้องเป็น a > b — baseline จึงแดงตั้งแต่ต้น
func Max(a, b int) int {
	if a < b {
		return a
	}
	return b
}
