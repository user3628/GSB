package pipeline

import "image"

func newGrayImage(w, h int, pix []uint8) *image.Gray {
	g := image.NewGray(image.Rect(0, 0, w, h))
	copy(g.Pix, pix)
	return g
}
