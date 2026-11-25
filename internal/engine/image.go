package engine

import (
	"fmt"
	"image"
	"image/png"
	"os"
)

type BinaryImage struct {
	Width     int
	Height    int
	Threshold int
	Ink       []bool
	InkCount  int
}

type Component struct {
	X0     int
	Y0     int
	X1     int // exclusive
	Y1     int // exclusive
	Pixels int
}

func (c Component) Width() int  { return c.X1 - c.X0 }
func (c Component) Height() int { return c.Y1 - c.Y0 }
func (c Component) CenterX() float64 {
	return float64(c.X0+c.X1) / 2
}
func (c Component) CenterY() float64 {
	return float64(c.Y0+c.Y1) / 2
}

func LoadPNG(path string) (image.Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	img, err := png.Decode(f)
	if err != nil {
		return nil, fmt.Errorf("decode png %s: %w", path, err)
	}
	return img, nil
}

func Binarize(img image.Image, threshold int, invert bool, strategy string) BinaryImage {
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	gray := make([]uint8, width*height)
	histogram := make([]int, 256)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			r, g, b, _ := img.At(bounds.Min.X+x, bounds.Min.Y+y).RGBA()
			value := uint8((299*int(r>>8) + 587*int(g>>8) + 114*int(b>>8)) / 1000)
			gray[y*width+x] = value
			histogram[value]++
		}
	}
	chosen := threshold
	if chosen <= 0 {
		chosen = otsu(histogram, width*height)
	}
	if strategy == "d-heavy" && chosen > 8 {
		// Heavy mode deliberately reserves a little more of the gray range for
		// low-contrast strokes. It does not use nondeterministic local state.
		chosen = minInt(255, chosen+6)
	}
	ink := make([]bool, width*height)
	count := 0
	for i, value := range gray {
		isInk := int(value) <= chosen
		if invert {
			isInk = !isInk
		}
		ink[i] = isInk
		if isInk {
			count++
		}
	}
	return BinaryImage{Width: width, Height: height, Threshold: chosen, Ink: ink, InkCount: count}
}

func otsu(hist []int, total int) int {
	if total == 0 {
		return 128
	}
	var sum float64
	for i, count := range hist {
		sum += float64(i * count)
	}
	var sumB, weightB float64
	bestVariance := -1.0
	best := 128
	for threshold := 0; threshold < 255; threshold++ {
		weightB += float64(hist[threshold])
		if weightB == 0 {
			continue
		}
		weightF := float64(total) - weightB
		if weightF == 0 {
			break
		}
		sumB += float64(threshold * hist[threshold])
		meanB := sumB / weightB
		meanF := (sum - sumB) / weightF
		variance := weightB * weightF * (meanB - meanF) * (meanB - meanF)
		if variance > bestVariance+1e-9 {
			bestVariance = variance
			best = threshold
		}
	}
	return best
}

func Segment(binary BinaryImage, strategy string, minPixels int) []Component {
	neighbors := [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}, {1, 1}, {1, -1}, {-1, 1}, {-1, -1}}
	if strategy == "d-light" {
		neighbors = neighbors[:4]
	}
	seen := make([]bool, len(binary.Ink))
	components := make([]Component, 0, 128)
	queue := make([]int, 0, 256)
	for index, ink := range binary.Ink {
		if !ink || seen[index] {
			continue
		}
		seen[index] = true
		queue = queue[:0]
		queue = append(queue, index)
		x0, y0 := binary.Width, binary.Height
		x1, y1 := 0, 0
		pixels := 0
		for head := 0; head < len(queue); head++ {
			current := queue[head]
			x := current % binary.Width
			y := current / binary.Width
			pixels++
			if x < x0 {
				x0 = x
			}
			if y < y0 {
				y0 = y
			}
			if x+1 > x1 {
				x1 = x + 1
			}
			if y+1 > y1 {
				y1 = y + 1
			}
			for _, delta := range neighbors {
				nx, ny := x+delta[0], y+delta[1]
				if nx < 0 || ny < 0 || nx >= binary.Width || ny >= binary.Height {
					continue
				}
				next := ny*binary.Width + nx
				if seen[next] || !binary.Ink[next] {
					continue
				}
				seen[next] = true
				queue = append(queue, next)
			}
		}
		if pixels >= minPixels {
			components = append(components, Component{X0: x0, Y0: y0, X1: x1, Y1: y1, Pixels: pixels})
		}
	}
	return components
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
