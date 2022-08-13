package slottedpage

import (
	"fmt"
	"os"
	"testing"
)

func TestCanWriteAdditionalInformationToPage(t *testing.T) {
	defer func() {
		os.Remove("test")
	}()
	p := PageManager{}

	err := p.WriteItemToPage("test", []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	err = p.WriteItemToPage("test", []byte("katie"))
	if err != nil {
		t.Error("expected no error")
	}

	pgInfo, err := p.ReadFromDisk("test")
	fmt.Println(pgInfo)

	if string(pgInfo.Slots[0].Item) != "edmund" {
		t.Errorf("expected string: %s, got: %s", "edmund", string(pgInfo.Slots[0].Item))
	}

	if string(pgInfo.Slots[1].Item) != "katie" {
		t.Errorf("expected string: %s, got: %s", "katie", string(pgInfo.Slots[1].Item))
	}

	if pgInfo.Items != 2 {
		t.Errorf("got incorrected item count, expected 2, got %d", pgInfo.Items)
	}
}

func TestCanWriteInitialItemToPage(t *testing.T) {
	defer func() {
		os.Remove("test2")
	}()
	p := PageManager{}

	err := p.WriteItemToPage("test2", []byte("edmund"))

	if err != nil {
		t.Error("expected no error")
	}

	page, err := p.ReadFromDisk("test2")

	if err != nil {
		t.Error("expected no error")
	}

	if string(page.Slots[0].Item) != "edmund" {
		t.Errorf("got unexpected value")
	}

	if page.Items != 1 {
		t.Errorf("got unexpected item count, expected 1, got: %d", page.Items)
	}
}

func TestCanDeleteSlotIDFromPage(t *testing.T) {
	testFile := "delete_slot_id_from_page"
	defer func() {
		os.Remove(testFile)
	}()

	p := PageManager{}

	err := p.WriteItemToPage(testFile, []byte("edmund"))

	if err != nil {
		t.Error("expected no error")
	}

	err = p.DeleteSlotIDFromPage(testFile, 0)
	if err != nil {
		t.Error("Expected no error")
	}

	page, err := p.ReadFromDisk(testFile)

	if page.Tombstones != 1 {
		t.Errorf("expected a tombstone")
	}

	if page.Items != 0 {
		t.Error("expected no items")
	}
}

func TestPageManager_DeleteSlotIDFromPage_MiddleIndex(t *testing.T) {
	testFile := "delete_middle_index"
	defer func() {
		os.Remove(testFile)
	}()
	p := PageManager{}

	err := p.WriteItemToPage(testFile, []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	err = p.WriteItemToPage(testFile, []byte("katie"))
	if err != nil {
		t.Error("expected no error")
	}

	err = p.WriteItemToPage(testFile, []byte("ronald"))
	if err != nil {
		t.Error("expected no error")
	}

	err = p.DeleteSlotIDFromPage(testFile, 1)
	if err != nil {
		t.Error("expected no error")
	}

	pgInfo, err := p.ReadFromDisk(testFile)
	if err != nil {
		t.Error("expected no error")
	}

	if pgInfo.Slots[1].Tombstone == false {
		t.Error("expected item to be tombstoned")
	}
}

func TestPageManager_ReadSlotIDFromDisk(t *testing.T) {
	testFile := "slot_id_from_disk"
	defer func() {
		os.Remove(testFile)
	}()
	p := PageManager{}

	err := p.WriteItemToPage(testFile, []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	err = p.WriteItemToPage(testFile, []byte("john"))
	if err != nil {
		t.Error("expected no error")
	}

	result, err := p.ReadSlotIDFromDisk(testFile, 1)
	if err != nil {
		t.Error("expected no error")
	}
	fmt.Println(result.Slots[0].Item)
	if string(result.Slots[0].Item) != "john" {
		t.Errorf("got unexpected value")
	}
}
