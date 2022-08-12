package slottedpage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	metaDataLength  = 1
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

		tombstone, err := readUint8At(page, startingOffset)
		if err != nil {
			return slotOffsets, err
		}
		startingOffset += uint8Length
		slotOffsets = append(slotOffsets, slotOffset{
			offset:    loc,
			size:      size,
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
	err := binary.Write(buffer, binary.BigEndian, uint8(len(items)))
	if err != nil {
		return nil, nil, err
	}
	startingIdx := 4000
	var offsets []int
	for _, item := range items {
		size := len(item)
		offset := startingIdx - size
		err = binary.Write(buffer, binary.BigEndian, uint16(offset))
		if err != nil {
			return nil, nil, err
		}
		// Can use uint16 here - as maximum possible storage not in a blob - less than uint16
		err = binary.Write(buffer, binary.BigEndian, uint16(size))
		if err != nil {
			return nil, nil, err
		}
		// Represents a tombstone
		err = binary.Write(buffer, binary.BigEndian, uint8(0))
		if err != nil {
			return nil, nil, err
		}
		startingIdx = offset
		offsets = append(offsets, offset)
	}
	return buffer.Bytes(), offsets, nil
}

func DeleteItemAtIndex(page []byte, idx int) ([]byte, error) {
	itemCount := int(page[0])
	if idx > itemCount-1 {
		return nil, errors.New("idx is not contained within page")
	}
	// Don't actually delete just tombstone record, compaction of page can deal with removing item
	byteToMod := metaDataLength
	if idx == 0 {
		byteToMod += 4
	} else {
		byteToMod += (5 * idx) + 4
	}
	page[byteToMod] = 1
	return page, nil
}

func UpdateItemAtIndex(page []byte, idx int) {
	// TODO - Given page bytes and idx update item at idx
	return
}

func WriteItemToPage(page []byte, item []byte) {
	// TODO - Given page bytes - write item to existing page
	return
}

func CompactPage(page []byte) {
	// TODO - Given a page as bytes - compact the page
	return
}

func calculatePageSize(items [][]byte) int {
	size := 1 + (5 * len(items))
	for _, item := range items {
		size += len(item)
	}
	return size
}
