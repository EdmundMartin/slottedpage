package slottedpage

func slotBinarySearch(arr []*Slot, left, right int, target int) int {
	if len(arr) == 0 {
		return -1
	}
	if left > right {
		return -1
	}
	middle := (left + right) / 2
	if arr[middle].SlotID == target {
		return middle
	} else if arr[middle].SlotID > target {
		return slotBinarySearch(arr, left, middle-1, target)
	} else {
		return slotBinarySearch(arr, middle+1, right, target)
	}
}
