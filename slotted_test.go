package slottedpage

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestSlottedPage_CanReadCreatedPage(t *testing.T) {

	dataOne := []string{"something", "interesting"}
	dataTwo := []string{"something", "else"}
	b, _ := json.Marshal(dataOne)
	b2, _ := json.Marshal(dataTwo)

	res, err := NewSlottedPage([][]byte{b, b2})
	if err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	p, err := ReadSlottedPage(res)
	if err != nil {
		t.Errorf("expected no error got %s", err)
	}
	if len(p.Items) != 2 {
		t.Errorf("expected two items in page, got %d", len(p.Items))
	}

	var resultOne []string
	var resultTwo []string
	json.Unmarshal(p.Items[0], &resultOne)
	json.Unmarshal(p.Items[1], &resultTwo)

	if !reflect.DeepEqual(dataOne, resultOne) {
		t.Errorf("data got manngled, expected: %v, got: %v", dataOne, resultOne)
	}

	if !reflect.DeepEqual(dataTwo, resultTwo) {
		t.Errorf("data got manngled, expected: %v, got: %v", dataTwo, resultTwo)
	}
}

func TestReadAndWriteSlottedPage(t *testing.T) {
	dataOne := []string{"something", "fun"}
	b, _ := json.Marshal(dataOne)

	m := PageManager{}

	err := m.WriteNewSlottedPage("test_page", [][]byte{b})
	defer func() {
		os.Remove("test_page")
	}()
	if err != nil {
		t.Errorf("expected no error")
	}

	p, err := m.ReadPageFromDisk("test_page")
	var resultOne []string
	json.Unmarshal(p.Items[0], &resultOne)

	if !reflect.DeepEqual(resultOne, dataOne) {
		t.Errorf("expected %v, got %v", dataOne, resultOne)
	}

}

func TestDeleteItemAtIndex_AtSlotIDZero(t *testing.T) {

	dataOne := []string{"something", "fun"}
	b, _ := json.Marshal(dataOne)

	page, err := NewSlottedPage([][]byte{b})
	if err != nil {
		t.Errorf("expected no error")
	}

	page, err = DeleteSlotItemByID(page, 0)

	result, err := ReadSlottedPage(page)
	if err != nil {
		t.Errorf("expected no error")
	}

	if result.CountItems != 0 {
		t.Errorf("expected zero item count, as removed only item, got: %d", result.CountItems)
	}

	if result.Tombstones != 1 {
		t.Errorf("expected a tombstone count of one as we deleted only item, got: %d", result.Tombstones)
	}
}

func TestDeleteItemAtIndex_AtNonZeroIndex(t *testing.T) {
	dataOne := []byte("hello world")
	dataTwo := []byte("Привет мир")
	dataThree := []byte("hola amigo")

	page, err := NewSlottedPage([][]byte{dataOne, dataTwo, dataThree})
	if err != nil {
		t.Errorf("expected no error")
	}

	page, err = DeleteSlotItemByID(page, 1)
	if err != nil {
		t.Errorf("expected no error")
	}

	result, err := ReadSlottedPage(page)
	if err != nil {
		t.Errorf("expected no error")
	}

	if result.CountItems != 2 {
		t.Errorf("expected two items in page, got: %d", result.CountItems)
	}

	if result.Tombstones != 1 {
		t.Errorf("expected one tombstone in page, got: %d", result.Tombstones)
	}

	if !reflect.DeepEqual(result.Items[0], dataOne) {
		t.Errorf("unexpected byte value for item one, got :%s", result.Items[0])
	}

	if !reflect.DeepEqual(result.Items[1], dataThree) {
		t.Errorf("unexpected byte value for item two, got :%s", result.Items[1])
	}
}

func TestDeleteItemAtIndex_NoSuchId(t *testing.T) {

}

func TestCompactPage(t *testing.T) {
	dataOne := []byte("hello world")
	dataTwo := []byte("Привет мир")
	dataThree := []byte("hola amigo")

	page, err := NewSlottedPage([][]byte{dataOne, dataTwo, dataThree})

	if err != nil {
		t.Errorf("expected no error")
	}

	// Delete the contents of the middle slot
	page, err = DeleteSlotItemByID(page, 1)
	if err != nil {
		t.Errorf("expected no error")
	}

	newPage, canDelete, err := CompactPage(page)

	if canDelete == true {
		t.Error("page cannot be deleted, as there are non tombstoned records")
	}

	p, err := ReadSlottedPage(newPage)
	if p.CountItems != 2 {
		t.Errorf("unpexted item count, expected 2, got %d", p.Items)
	}

	if string(dataOne) != string(p.Items[0]) {
		t.Errorf("expected the same string values")
	}

	if string(dataThree) != string(p.Items[1]) {
		t.Errorf("expected the same string values")
	}
}

func TestCanWriteValueToExistingPage(t *testing.T) {
	dataOne := []byte("hello world")
	dataTwo := []byte("Привет мир")
	dataThree := []byte("hola amigo")

	page, err := NewSlottedPage([][]byte{dataOne, dataTwo, dataThree})
	if err != nil {
		t.Error("expected no error")
	}

	err = WriteItemToPage(page, []byte("Ronald McDonald"))
	fmt.Println(err)

	p, err := ReadSlottedPage(page)

	if p.CountItems != 4 {
		t.Errorf("expected 4 items got, %d", p.CountItems)
	}

	expected := []string{
		"hello world",
		"Привет мир",
		"hola amigo",
		"Ronald McDonald",
	}
	for idx, item := range p.Items {
		if expected[idx] != string(item) {
			t.Errorf("unexpected value at idx: %d, got %s, expected %s", idx, string(item), expected[idx])
		}
	}
}
