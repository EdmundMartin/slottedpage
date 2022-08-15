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

	slotID, err := p.WriteItemToPage("test", []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	if slotID != 0 {
		t.Error("expected slot ID of zero")
	}

	slotID, err = p.WriteItemToPage("test", []byte("katie"))
	if err != nil {
		t.Error("expected no error")
	}

	if slotID != 1 {
		t.Error("expected slot ID of one")
	}

	pgInfo, err := p.ReadFromDisk("test")

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

	_, err := p.WriteItemToPage("test2", []byte("edmund"))

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

	_, err := p.WriteItemToPage(testFile, []byte("edmund"))

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

	_, err := p.WriteItemToPage(testFile, []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	_, err = p.WriteItemToPage(testFile, []byte("katie"))
	if err != nil {
		t.Error("expected no error")
	}

	_, err = p.WriteItemToPage(testFile, []byte("ronald"))
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

	_, err := p.WriteItemToPage(testFile, []byte("edmund"))
	if err != nil {
		t.Error("expected no error")
	}

	_, err = p.WriteItemToPage(testFile, []byte("john"))
	if err != nil {
		t.Error("expected no error")
	}

	result, err := p.ReadSlotIDFromDisk(testFile, 1)
	if err != nil {
		t.Error("expected no error")
	}

	if string(result.Slots[0].Item) != "john" {
		t.Errorf("got unexpected value")
	}
}

func TestUpdateItem_ItemHasSameByteSize(t *testing.T) {

	testFile := "update_same_size_value"
	valOne := []byte("joey")
	valTwo := []byte("john")

	defer func() {
		os.Remove(testFile)
	}()

	pg := PageManager{}

	slotID, err := pg.WriteItemToPage(testFile, valOne)
	if err != nil {
		t.Error("expected no error")
	}

	err = pg.UpdateItem(testFile, slotID, valTwo)
	if err != nil {
		fmt.Println(err)
		t.Error("expected no error")
	}

	contents, err := pg.ReadFromDisk(testFile)
	if err != nil {
		t.Error("expected no error")
	}

	if string(contents.Slots[0].Item) != "john" {
		t.Errorf("expected john, got %s", string(contents.Slots[0].Item))
	}
}

func TestUpdateItem_ItemHasSmallerNumberBytes(t *testing.T) {
	testFile := "update_same_smaller_size_value"
	valOne := []byte("joey")
	valTwo := []byte("bob")

	defer func() {
		os.Remove(testFile)
	}()

	pg := PageManager{}

	slotID, err := pg.WriteItemToPage(testFile, valOne)
	if err != nil {
		t.Error("expected no error")
	}

	err = pg.UpdateItem(testFile, slotID, valTwo)
	if err != nil {
		t.Error("expected no error")
	}

	contents, err := pg.ReadFromDisk(testFile)
	if err != nil {
		t.Error("expected no error")
	}

	if string(contents.Slots[0].Item) != "bob" {
		t.Errorf("expected bob, got %s", string(contents.Slots[0].Item))
	}

	if contents.Slots[0].Size != 3 {
		t.Error("expected 'bob' to be 3 bytes in length")
	}
}

func TestUpdateItem_ItemHasLargerNumberBytes(t *testing.T) {
	testFile := "update_same_larger_size_value"
	valOne := []byte("ron")
	valTwo := []byte("jon")
	valThree := []byte("con")

	longerJon := []byte("john")

	defer func() {
		os.Remove(testFile)
	}()

	pg := PageManager{}

	_, err := pg.WriteItemToPage(testFile, valOne)
	if err != nil {
		t.Error("expected no error")
	}

	slotID, err := pg.WriteItemToPage(testFile, valTwo)
	if err != nil {
		t.Error("expected no error")
	}

	_, err = pg.WriteItemToPage(testFile, valThree)
	if err != nil {
		t.Error("expected no error")
	}

	err = pg.UpdateItem(testFile, slotID, longerJon)
	if err != nil {
		t.Error("expected no error")
	}

	contents, err := pg.ReadFromDisk(testFile)
	if err != nil {
		t.Error("expected no error")
	}

	if string(contents.Slots[0].Item) != "ron" {
		t.Errorf("expected ron, got %s", string(contents.Slots[0].Item))
	}

	if contents.Slots[0].Size != 3 {
		t.Error("expected 'ron' to be 3 bytes in length")
	}

	if string(contents.Slots[1].Item) != "john" {
		t.Errorf("expected john, got %s", string(contents.Slots[1].Item))
	}

	if contents.Slots[1].Size != 4 {
		t.Error("expected 'john' to be 4 bytes in length")
	}

	if string(contents.Slots[2].Item) != "con" {
		t.Errorf("expected con, got %s", string(contents.Slots[2].Item))
	}

	if contents.Slots[2].Size != 3 {
		t.Error("expected 'con' to be 4 bytes in length")
	}

}
