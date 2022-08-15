package slottedpage

import (
	"encoding/binary"
	"errors"
	"os"
)

const (
	metaDataLength  = 5
	slotInfoSize    = 9
	uint8Length     = 1
	uint16Length    = 2
	uint32Length    = 4
	defaultPageSize = 4000
)

// TODO
// Reimplement compaction
// Update value at slot

type MetaData struct {
	ItemCount uint8
	LastID    uint32
}

func readHeadersFromFile(file *os.File) (*MetaData, error) {
	itemCount, err := fileReadUint8At(file, 0)
	if err != nil {
		return nil, err
	}

	id, err := fileReadUint32At(file, 1)
	if err != nil {
		return nil, err
	}
	m := &MetaData{
		ItemCount: itemCount,
		LastID:    id,
	}
	return m, nil
}

type Slot struct {
	Idx       int
	Offset    int
	Size      int
	SlotID    int
	Tombstone bool
	Item      []byte
}

type PageInformation struct {
	MetaData   *MetaData
	Slots      []*Slot
	Items      int
	Tombstones int
}

func readPageAtSpecificSlot(file *os.File, slotID int) (*PageInformation, error) {
	metaData, err := readHeadersFromFile(file)
	if err != nil {
		return nil, err
	}
	slots, err := readSlotInfo(file, int(metaData.ItemCount))
	pgInfo := &PageInformation{
		MetaData:   metaData,
		Slots:      []*Slot{},
		Items:      0,
		Tombstones: 0,
	}
	slotIdx := slotBinarySearch(slots, 0, len(slots)-1, slotID)
	if slotIdx < 0 {
		return nil, errors.New("no such slot in page")
	}
	pgInfo.Items++
	targetSlot := slots[slotIdx]
	rawBytes := make([]byte, targetSlot.Size)

	_, err = file.ReadAt(rawBytes, int64(targetSlot.Offset))
	if err != nil {
		return nil, err
	}

	targetSlot.Item = rawBytes
	pgInfo.Slots = append(pgInfo.Slots, targetSlot)
	return pgInfo, nil
}

func readPageNew(file *os.File) (*PageInformation, error) {
	metaData, err := readHeadersFromFile(file)
	if err != nil {
		return nil, err
	}
	slots, err := readSlotInfo(file, int(metaData.ItemCount))

	pgInfo := &PageInformation{
		MetaData:   metaData,
		Slots:      slots,
		Items:      0,
		Tombstones: 0,
	}
	for _, slot := range slots {
		if slot.Tombstone {
			pgInfo.Tombstones++
			continue
		}
		pgInfo.Items++
		rawBytes := make([]byte, slot.Size)
		_, err = file.ReadAt(rawBytes, int64(slot.Offset))
		if err != nil {
			return nil, err
		}
		slot.Item = rawBytes
	}
	return pgInfo, nil
}

func fileReadUint16At(file *os.File, offset int64) (uint16, error) {
	store := make([]byte, 2)

	_, err := file.ReadAt(store, offset)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(store), nil
}

func fileReadUint32At(file *os.File, offset int64) (uint32, error) {
	store := make([]byte, 4)

	_, err := file.ReadAt(store, offset)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(store), nil
}

func fileReadUint8At(file *os.File, offset int64) (uint8, error) {
	store := make([]byte, 1)
	_, err := file.ReadAt(store, offset)
	if err != nil {
		return 0, err
	}
	return store[0], nil
}

func readSlotInfo(file *os.File, itemCount int) ([]*Slot, error) {
	var slotInfo []*Slot
	pos := int64(metaDataLength)
	for i := 0; i < itemCount; i++ {

		offset, err := fileReadUint16At(file, pos)
		if err != nil {
			return nil, err
		}
		pos += uint16Length

		size, err := fileReadUint16At(file, pos)
		if err != nil {
			return nil, err
		}
		pos += uint16Length

		slotID, err := fileReadUint32At(file, pos)
		if err != nil {
			return nil, err
		}
		pos += uint32Length

		tombstone, err := fileReadUint8At(file, pos)
		if err != nil {
			return nil, err
		}

		pos += uint8Length
		slotInfo = append(slotInfo, &Slot{
			Idx:       i,
			Offset:    int(offset),
			Size:      int(size),
			SlotID:    int(slotID),
			Tombstone: tombstone == 1,
		})
	}

	return slotInfo, nil
}

func calculateTombstoneByte(idx int) int {
	if idx == 0 {
		return metaDataLength + slotInfoSize - 1
	}
	return metaDataLength + (slotInfoSize * idx) + (slotInfoSize - 1)
}

func deleteItemAtSlotID(file *os.File, slotID int) error {
	metaData, err := readHeadersFromFile(file)
	if uint32(slotID) > metaData.LastID {
		return nil
	}
	slots, err := readSlotInfo(file, int(metaData.ItemCount))
	if err != nil {
		return err
	}

	foundIdx := slotBinarySearch(slots, 0, len(slots)-1, slotID)

	// Slot with the ID is not found in the page simply return
	if foundIdx == -1 {
		return nil
	}
	tombstoneLocation := calculateTombstoneByte(foundIdx)
	_, err = file.WriteAt([]byte{1}, int64(tombstoneLocation))
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func compactPageUpdate(file *os.File, slotId int, newBytesVale []byte) error {
	metaData, err := readHeadersFromFile(file)
	if err != nil {
		return err
	}

	slots, err := readSlotInfo(file, int(metaData.ItemCount))

	for _, slot := range slots {
		if slot.SlotID == slotId {
			slot.Item = newBytesVale
			slot.Size = len(newBytesVale)
		} else {
			bytes := make([]byte, slot.Size)
			_, err := file.ReadAt(bytes, int64(slot.Offset))
			if err != nil {
				return err
			}
			slot.Item = bytes
		}
	}
	startLeft := metaDataLength
	startRight := defaultPageSize

	for _, slot := range slots {
		if slot.Tombstone {
			continue
		}
		offset := startRight - slot.Size

		err = writeSlotInfoToFile(file, int64(startLeft), uint16(offset), uint16(slot.Size), uint32(slot.SlotID), 0)
		if err != nil {
			return err
		}
		startLeft += slotInfoSize

		_, err = file.WriteAt(slot.Item, int64(offset))
		if err != nil {
			return err
		}
		startRight = offset
	}

	return nil
}

func updateItem(file *os.File, targetSlotID int, item []byte) error {

	pgInfo, err := readPageAtSpecificSlot(file, targetSlotID)
	if err != nil {
		return err
	}

	// If found will always be Idx zero
	slot := pgInfo.Slots[0]

	if slot.Size == len(item) {
		_, err = file.WriteAt(item, int64(slot.Offset))
		if err != nil {
			return err
		}
		err = file.Sync()
		if err != nil {
			return err
		}
		return nil
	}

	if len(item) <= slot.Size {
		_, err = file.WriteAt(item, int64(slot.Offset))
		if err != nil {
			return err
		}

		sizeOffset := calculateSizeByteOffset(slot.Idx)

		err = writeUint16At(file, sizeOffset, uint16(len(item)))
		if err != nil {
			return err
		}

		err = file.Sync()
		if err != nil {
			return err
		}
		return nil
	}

	if err := compactPageUpdate(file, targetSlotID, item); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func writeItemToPage(file *os.File, item []byte) (int, error) {
	metaData, err := readHeadersFromFile(file)
	if err != nil {
		return -1, err
	}
	var lastOffset int
	var off uint16
	if metaData.ItemCount == 0 {
		lastOffset = metaDataLength
		off = defaultPageSize
	} else {
		lastOffset = int(metaData.ItemCount)*slotInfoSize - slotInfoSize + metaDataLength
		offset := make([]byte, 2)
		_, err = file.ReadAt(offset, int64(lastOffset))
		if err != nil {
			return -1, err
		}
		off = binary.BigEndian.Uint16(offset)
		hasSpace := int(off) - (metaDataLength + (int(metaData.ItemCount) * slotInfoSize))
		if len(item) > hasSpace {
			return -1, errors.New("not enough space")
		}
	}

	newOffset := int(off) - len(item)
	_, err = file.WriteAt(item, int64(newOffset))
	if err != nil {
		return -1, err
	}
	// Update item count
	_, err = file.WriteAt([]byte{metaData.ItemCount + 1}, 0)

	newSlotLoc := (slotInfoSize * int(metaData.ItemCount)) + metaDataLength
	err = writeSlotInfoToFile(file, int64(newSlotLoc), uint16(newOffset), uint16(len(item)), metaData.LastID, 0)
	if err != nil {
		return -1, err
	}

	err = writeUint32At(file, 1, metaData.LastID+1)
	if err != nil {
		return -1, err
	}

	if err := file.Sync(); err != nil {
		return -1, err
	}
	return int(metaData.LastID), nil
}

func writeUint16At(file *os.File, pos int64, value uint16) error {
	dataToWrite := make([]byte, uint16Length)
	binary.BigEndian.PutUint16(dataToWrite, value)
	if _, err := file.WriteAt(dataToWrite, pos); err != nil {
		return err
	}
	return nil
}

func writeUint32At(file *os.File, pos int64, value uint32) error {
	dataToWrite := make([]byte, uint32Length)
	binary.BigEndian.PutUint32(dataToWrite, value)
	if _, err := file.WriteAt(dataToWrite, pos); err != nil {
		return err
	}
	return nil
}

func writeSlotInfoToFile(f *os.File, pos int64, offset, size uint16, slotID uint32, tombstone uint8) error {
	if err := writeUint16At(f, pos, offset); err != nil {
		return err
	}

	pos += uint16Length

	if err := writeUint16At(f, pos, size); err != nil {
		return err
	}

	pos += uint16Length

	if err := writeUint32At(f, pos, slotID); err != nil {
		return err
	}
	pos += uint32Length

	if _, err := f.WriteAt([]byte{tombstone}, pos); err != nil {
		return err
	}
	return nil
}

func calculateSizeByteOffset(idx int) int64 {
	if idx == 0 {
		return int64(metaDataLength + slotInfoSize - 7)
	}
	return int64(metaDataLength + (slotInfoSize * idx) + slotInfoSize - 7)
}
