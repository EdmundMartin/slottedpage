package slottedpage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	metaDataLength  = 5
	slotInfoSize    = 9
	uint8Length     = 1
	uint16Length    = 2
	uint32Length    = 4
	defaultPageSize = 4000
)

type Page struct {
	CountItems int
	Items      [][]byte
	Tombstones int
	RawPage    []byte
}

func (p *Page) Size() int {
	size := 1 + (len(p.Items) * 4)
	for _, item := range p.Items {
		size += len(item)
	}
	return size
}

func NewSlottedPage(items [][]byte) ([]byte, error) {
	if len(items) > 255 {
		// Item count is uint8 - allowing for 255 positive values
		return nil, errors.New("max 255 items can be stored in single page")
	}
	diskUsage := calculatePageSize(items)
	// TODO - If a single item is very large we should store in file blob
	// TODO - The page would then simply store a pointer to the file blob to be loaded
	// TODO - Check the disk size - assuming we are storing these large items in blob overflows
	if diskUsage > defaultPageSize {
		return nil, errors.New("items would overflow page size")
	}
	headerBytes, offsets, err := buildHeaders(items)
	if err != nil {
		return nil, err
	}
	page := make([]byte, defaultPageSize)
	writeBytesAt(page, 0, headerBytes)
	for idx, item := range items {
		writeBytesAt(page, offsets[idx], item)
	}
	return page, nil
}

type slotOffset struct {
	offset    uint16
	size      uint16
	slotID    uint32
	tombstone uint8
}

func ReadSlottedPage(page []byte) (*Page, error) {
	reader := bytes.NewReader(page)
	itemCount, err := readUint8At(reader, 0)
	if err != nil {
		return nil, err
	}
	offsets, err := readSlotInformation(reader, int(itemCount), metaDataLength)
	if err != nil {
		return nil, err
	}
	parsedPage := &Page{
		CountItems: int(itemCount),
		Items:      [][]byte{},
		RawPage:    page,
		Tombstones: 0,
	}
	for _, off := range offsets {
		if off.tombstone == 1 {
			// Don't bother reading tomb stoned records
			parsedPage.Tombstones += 1
			parsedPage.CountItems -= 1
			continue
		}
		rawBytes, err := readNBytesAt(reader, int64(off.offset), int(off.size))
		if err != nil {
			return nil, err
		}
		parsedPage.Items = append(parsedPage.Items, rawBytes)
	}
	return parsedPage, nil
}

func readNBytesAt(reader *bytes.Reader, offset int64, size int) ([]byte, error) {
	store := make([]byte, size)
	_, err := reader.ReadAt(store, offset)
	return store, err
}

func readUint32At(reader *bytes.Reader, offset int64) (uint32, error) {
	data, err := readNBytesAt(reader, offset, uint32Length)
	if err != nil {
		return 0, err
	}
	var result uint32
	dataReader := bytes.NewReader(data)
	err = binary.Read(dataReader, binary.BigEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, err
}

func writeUint32At(page []byte, number uint32, offset int64) error {
	tmpBuffer := new(bytes.Buffer)
	err := binary.Write(tmpBuffer, binary.BigEndian, number)
	if err != nil {
		return err
	}
	copyRecord(0, int(offset), 4, tmpBuffer.Bytes(), page)
	return nil
}

func readUint16At(reader *bytes.Reader, offset int64) (uint16, error) {
	data, err := readNBytesAt(reader, offset, uint16Length)
	if err != nil {
		return 0, err
	}
	var result uint16
	dataReader := bytes.NewReader(data)
	err = binary.Read(dataReader, binary.BigEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, err
}

func readUint8At(reader *bytes.Reader, offset int64) (uint8, error) {
	data, err := readNBytesAt(reader, offset, 1)
	if err != nil {
		return 0, err
	}
	var result uint8
	dataReader := bytes.NewReader(data)
	err = binary.Read(dataReader, binary.BigEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, err
}

func readSlotInformation(page *bytes.Reader, itemCount int, startingOffset int64) ([]slotOffset, error) {
	var slotOffsets []slotOffset
	for i := 0; i < itemCount; i++ {
		loc, err := readUint16At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += uint16Length
		size, err := readUint16At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += uint16Length

		slotID, err := readUint32At(page, startingOffset)
		if err != nil {
			return nil, err
		}
		startingOffset += uint32Length

		tombstone, err := readUint8At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += uint8Length
		slotOffsets = append(slotOffsets, slotOffset{
			offset:    loc,
			size:      size,
			slotID:    slotID,
			tombstone: tombstone,
		})
	}
	return slotOffsets, nil
}

func writeBytesAt(page []byte, offsetIdx int, toWrite []byte) {
	for i := 0; i < len(toWrite); i++ {
		page[offsetIdx+i] = toWrite[i]
	}
}

func buildHeaders(items [][]byte) ([]byte, []int, error) {
	buffer := new(bytes.Buffer)
	// Count items
	err := binary.Write(buffer, binary.BigEndian, uint8(len(items)))
	if err != nil {
		return nil, nil, err
	}
	// Slot ID counter
	err = binary.Write(buffer, binary.BigEndian, uint32(len(items)))
	if err != nil {
		return nil, nil, err
	}

	startingIdx := 4000
	var offsets []int
	for idx, item := range items {
		size := len(item)
		offset := startingIdx - size
		err := writeSlotInfo(buffer, uint16(offset), uint16(size), uint32(idx), 0)
		if err != nil {
			return nil, nil, err
		}
		startingIdx = offset
		offsets = append(offsets, offset)
	}
	return buffer.Bytes(), offsets, nil
}

func DeleteSlotItemByID(page []byte, id int) ([]byte, error) {
	itemCount := int(page[0])
	reader := bytes.NewReader(page)
	slots, err := readSlotInformation(reader, itemCount, metaDataLength)
	if err != nil {
		return nil, err
	}

	// TODO - Let's do binary search
	slotIdx := -1
	for idx, slot := range slots {
		if int(slot.slotID) == id {
			slotIdx = idx
		}
	}
	if slotIdx < 0 {
		return nil, errors.New("slot id is not contained within the page")
	}
	var byteToMod int
	if slotIdx == 0 {
		byteToMod = metaDataLength + (slotInfoSize - 1)
	} else {
		byteToMod = metaDataLength + (slotInfoSize * slotIdx) + (slotInfoSize - 1)
	}
	page[byteToMod] = 1
	return page, nil
}

func UpdateItemWithSlotID(page []byte, slotID int, item []byte) {
	// TODO - Given page bytes and idx update item at idx
	return
}

func WriteItemToPage(page []byte, item []byte) error {
	// TODO - Given page bytes - write item to existing page
	itemCount := int(page[0])
	reader := bytes.NewReader(page)

	lastID, err := readUint32At(reader, 1)
	if err != nil {
		return err
	}

	if itemCount == 0 {
		pageSize := calculatePageSize([][]byte{item})
		if pageSize > defaultPageSize {
			// TODO - Lets handle blobs
			return errors.New("not enough space to write item to page")
		}
		tmpBuffer := new(bytes.Buffer)
		size := len(item)
		idx := defaultPageSize - size
		copyRecord(0, idx, size, item, page)
		err = writeSlotInfo(tmpBuffer, uint16(idx), uint16(size), lastID, 0)
		if err != nil {
			return err
		}
		writeBytesAt(page, metaDataLength, tmpBuffer.Bytes())

		page[0] = 1
		lastID += 1
		err = writeUint32At(page, lastID, 1)
		if err != nil {
			return err
		}

		return nil
	}

	slots, err := readSlotInformation(reader, itemCount, metaDataLength)
	finalSlot := slots[len(slots)-1]
	space := int(finalSlot.offset) - (metaDataLength + (itemCount * slotInfoSize) + slotInfoSize)
	if len(item) > space {
		return errors.New("not enough space to write into page")
	}
	tmpBuffer := new(bytes.Buffer)
	size := len(item)
	idx := int(finalSlot.offset) - size
	copyRecord(0, idx, size, item, page)

	err = writeSlotInfo(tmpBuffer, uint16(idx), uint16(size), lastID, uint8(0))
	if err != nil {
		return err
	}
	startIdx := metaDataLength + (itemCount * slotInfoSize)
	copyRecord(0, startIdx, slotInfoSize, tmpBuffer.Bytes(), page)
	page[0] += 1
	lastID += 1
	err = writeUint32At(page, lastID, 1)
	if err != nil {
		return err
	}
	return nil
}

func CompactPage(page []byte) ([]byte, bool, error) {
	countItems := int(page[0])
	reader := bytes.NewReader(page)
	slots, err := readSlotInformation(reader, countItems, metaDataLength)
	if err != nil {
		return nil, false, err
	}
	tombstoned := 0
	var slotsToKeep []slotOffset
	for _, slot := range slots {
		if slot.tombstone == 1 {
			tombstoned += 1
			continue
		}
		slotsToKeep = append(slotsToKeep, slot)
	}
	if tombstoned == 0 {
		return page, false, nil
	}
	if len(slotsToKeep) == 0 {
		// If page has only tombstoned records we can delete the page from disk
		return nil, true, nil
	}
	newPage := make([]byte, defaultPageSize)
	// Count items will be the count of non tombstoned items
	newPage[0] = uint8(len(slotsToKeep))
	// Copy the slot ID count over - so systems accessing our page can still access items by slot ID
	// despite the disk location having changed
	copyByteRange(uint8Length, 4, page, newPage)
	rightIdx := defaultPageSize
	tmpBuffer := new(bytes.Buffer)
	for _, slot := range slotsToKeep {
		idx := rightIdx - int(slot.size)
		copyRecord(int(slot.offset), idx, int(slot.size), page, newPage)
		rightIdx = idx
		err := writeSlotInfo(tmpBuffer, uint16(rightIdx), slot.size, slot.slotID, slot.tombstone)
		if err != nil {
			return nil, false, err
		}
	}
	writeBytesAt(newPage, metaDataLength, tmpBuffer.Bytes())
	return newPage, false, err
}

func copyRecord(oldStart, newStart, size int, src, dst []byte) {
	for i := 0; i < size; i++ {
		dst[newStart+i] = src[oldStart+i]
	}
}

func copyByteRange(startIdx int, count int, src, dst []byte) {
	for i := 0; i < count; i++ {
		dst[startIdx+i] = src[startIdx+i]
	}
}

func calculatePageSize(items [][]byte) int {
	size := metaDataLength + (slotInfoSize * len(items))
	for _, item := range items {
		size += len(item)
	}
	return size
}

func writeSlotInfo(buffer *bytes.Buffer, offset, size uint16, slotID uint32, tombstone uint8) error {
	// Offset
	err := binary.Write(buffer, binary.BigEndian, offset)
	if err != nil {
		return err
	}
	// Can use uint16 here - as maximum possible storage not in a blob - less than uint16
	err = binary.Write(buffer, binary.BigEndian, size)
	if err != nil {
		return err
	}
	// Slot ID - unint32 - as we want to keep slot IDs unique regardless of compaction
	// Allows a page to be written to 4 billion times
	err = binary.Write(buffer, binary.BigEndian, slotID)
	if err != nil {
		return err
	}
	// Represents a tombstone
	err = binary.Write(buffer, binary.BigEndian, tombstone)
	if err != nil {
		return err
	}
	return nil
}
