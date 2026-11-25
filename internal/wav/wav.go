package wav

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type PCM16 struct {
	SampleRate int
	Channels   int
	Samples    []int16
}

func EncodePCM16(samples []int16, sampleRate, channels int) ([]byte, error) {
	if channels != 1 {
		return nil, fmt.Errorf("only mono PCM16 is supported, got %d channels", channels)
	}
	dataBytes := len(samples) * 2
	buf := make([]byte, 44+dataBytes)
	copy(buf[0:4], []byte("RIFF"))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(36+dataBytes))
	copy(buf[8:12], []byte("WAVE"))
	copy(buf[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(buf[16:20], 16)
	binary.LittleEndian.PutUint16(buf[20:22], 1)
	binary.LittleEndian.PutUint16(buf[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(sampleRate))
	byteRate := uint32(sampleRate * channels * 16 / 8)
	binary.LittleEndian.PutUint32(buf[28:32], byteRate)
	blockAlign := uint16(channels * 2)
	binary.LittleEndian.PutUint16(buf[32:34], blockAlign)
	binary.LittleEndian.PutUint16(buf[34:36], 16)
	copy(buf[36:40], []byte("data"))
	binary.LittleEndian.PutUint32(buf[40:44], uint32(dataBytes))
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(buf[44+i*2:46+i*2], uint16(sample))
	}
	return buf, nil
}

func WriteFile(path string, samples []int16, sampleRate int) error {
	data, err := EncodePCM16(samples, sampleRate, 1)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func DecodePCM16(data []byte) (PCM16, error) {
	if len(data) < 44 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return PCM16{}, fmt.Errorf("invalid WAV/RIFF header")
	}

	offset := 12
	sampleRate := 0
	channels := 0
	bits := 0
	var pcm []byte

	for offset+8 <= len(data) {
		id := string(data[offset : offset+4])
		size := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		body := offset + 8
		if size < 0 || body+size > len(data) {
			return PCM16{}, fmt.Errorf("truncated WAV chunk %q", id)
		}
		switch id {
		case "fmt ":
			if size < 16 {
				return PCM16{}, fmt.Errorf("short fmt chunk")
			}
			format := binary.LittleEndian.Uint16(data[body : body+2])
			if format != 1 {
				return PCM16{}, fmt.Errorf("unsupported WAV format %d", format)
			}
			channels = int(binary.LittleEndian.Uint16(data[body+2 : body+4]))
			sampleRate = int(binary.LittleEndian.Uint32(data[body+4 : body+8]))
			bits = int(binary.LittleEndian.Uint16(data[body+14 : body+16]))
		case "data":
			pcm = data[body : body+size]
		}
		offset = body + size + (size & 1)
	}

	if sampleRate <= 0 || channels <= 0 || bits != 16 {
		return PCM16{}, fmt.Errorf("unsupported WAV format: rate=%d channels=%d bits=%d", sampleRate, channels, bits)
	}
	if channels != 1 {
		return PCM16{}, fmt.Errorf("only mono input is supported, got %d", channels)
	}
	if len(pcm)%2 != 0 {
		return PCM16{}, fmt.Errorf("PCM data is not 16-bit aligned")
	}
	samples := make([]int16, len(pcm)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(pcm[i*2 : i*2+2]))
	}
	return PCM16{SampleRate: sampleRate, Channels: channels, Samples: samples}, nil
}

func ReadFile(path string) (PCM16, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return PCM16{}, nil, err
	}
	pcm, err := DecodePCM16(data)
	if err != nil {
		return PCM16{}, nil, err
	}
	return pcm, data, nil
}

func ReadAllPCM16(r io.Reader) (PCM16, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return PCM16{}, err
	}
	return DecodePCM16(data)
}
