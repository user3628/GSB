package cockpitaudio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type WAV struct {
	SampleRateHz  int
	Channels      int
	BitsPerSample int
	Samples       []int16
}

func WriteWAV(path string, w *WAV) error {
	if w.Channels != 1 || w.BitsPerSample != 16 {
		return fmt.Errorf("only mono 16-bit WAV is supported")
	}
	dataBytes := len(w.Samples) * 2
	hdr := make([]byte, 44)
	copy(hdr[0:4], []byte("RIFF"))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(36+dataBytes))
	copy(hdr[8:12], []byte("WAVE"))
	copy(hdr[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(hdr[16:20], 16)
	binary.LittleEndian.PutUint16(hdr[20:22], 1)
	binary.LittleEndian.PutUint16(hdr[22:24], uint16(w.Channels))
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(w.SampleRateHz))
	byteRate := uint32(w.SampleRateHz * w.Channels * w.BitsPerSample / 8)
	binary.LittleEndian.PutUint32(hdr[28:32], byteRate)
	blockAlign := uint16(w.Channels * w.BitsPerSample / 8)
	binary.LittleEndian.PutUint16(hdr[32:34], blockAlign)
	binary.LittleEndian.PutUint16(hdr[34:36], uint16(w.BitsPerSample))
	copy(hdr[36:40], []byte("data"))
	binary.LittleEndian.PutUint32(hdr[40:44], uint32(dataBytes))

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	buf := make([]byte, dataBytes)
	for i, s := range w.Samples {
		binary.LittleEndian.PutUint16(buf[i*2:i*2+2], uint16(s))
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	return f.Close()
}

func ReadWAV(path string) (*WAV, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 12 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, fmt.Errorf("invalid WAV container")
	}

	pos := 12
	var sampleRate, byteRate uint32
	var channels, bits uint16
	var data []byte
	for pos+8 <= len(b) {
		id := string(b[pos : pos+4])
		size := int(binary.LittleEndian.Uint32(b[pos+4 : pos+8]))
		bodyStart := pos + 8
		bodyEnd := bodyStart + size
		if bodyEnd > len(b) {
			return nil, io.ErrUnexpectedEOF
		}
		switch id {
		case "fmt ":
			if size < 16 {
				return nil, fmt.Errorf("short fmt chunk")
			}
			channels = binary.LittleEndian.Uint16(b[bodyStart+2 : bodyStart+4])
			sampleRate = binary.LittleEndian.Uint32(b[bodyStart+4 : bodyStart+8])
			byteRate = binary.LittleEndian.Uint32(b[bodyStart+8 : bodyStart+12])
			bits = binary.LittleEndian.Uint16(b[bodyStart+14 : bodyStart+16])
		case "data":
			data = b[bodyStart:bodyEnd]
		}
		pos = bodyEnd + (size & 1)
	}
	if sampleRate == 0 || data == nil {
		return nil, fmt.Errorf("missing fmt/data chunk")
	}
	if channels != 1 || bits != 16 {
		return nil, fmt.Errorf("only mono 16-bit WAV is supported")
	}
	if byteRate == 0 {
		byteRate = sampleRate * uint32(channels) * uint32(bits) / 8
	}
	samples := make([]int16, len(data)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2 : i*2+2]))
	}
	return &WAV{
		SampleRateHz:  int(sampleRate),
		Channels:      int(channels),
		BitsPerSample: int(bits),
		Samples:       samples,
	}, nil
}
