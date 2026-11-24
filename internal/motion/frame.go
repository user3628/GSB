package motion

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"sync"

	"motionlab/internal/model"
)

type Frame struct {
	Width  int    `json:"width"`
	Height int    `json:"height"`
	RGB    []byte `json:"-"`
	SHA256 string `json:"sha256"`
}

func NewFrame(width, height int, rgb []byte) *Frame {
	cp := append([]byte(nil), rgb...)
	return &Frame{Width: width, Height: height, RGB: cp, SHA256: model.DigestBytes(cp)}
}

func (f *Frame) Clone() *Frame {
	if f == nil {
		return nil
	}
	return NewFrame(f.Width, f.Height, f.RGB)
}

func (f *Frame) PNG() ([]byte, error) {
	img := image.NewNRGBA(image.Rect(0, 0, f.Width, f.Height))
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			i := (y*f.Width + x) * 3
			img.SetNRGBA(x, y, color.NRGBA{R: f.RGB[i], G: f.RGB[i+1], B: f.RGB[i+2], A: 255})
		}
	}
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestCompression}
	if err := enc.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodePNG(data []byte) (*Frame, error) {
	img, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	b := img.Bounds()
	rgb := make([]byte, b.Dx()*b.Dy()*3)
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			i := ((y-b.Min.Y)*b.Dx() + (x - b.Min.X)) * 3
			rgb[i] = uint8(r >> 8)
			rgb[i+1] = uint8(g >> 8)
			rgb[i+2] = uint8(bl >> 8)
		}
	}
	return NewFrame(b.Dx(), b.Dy(), rgb), nil
}

func MAE(a, b *Frame) (float64, error) {
	if a == nil || b == nil {
		return 0, fmt.Errorf("nil frame")
	}
	if a.Width != b.Width || a.Height != b.Height || len(a.RGB) != len(b.RGB) {
		return 0, fmt.Errorf("frame dimensions differ: %dx%d and %dx%d", a.Width, a.Height, b.Width, b.Height)
	}
	var sum uint64
	for i := range a.RGB {
		if a.RGB[i] > b.RGB[i] {
			sum += uint64(a.RGB[i] - b.RGB[i])
		} else {
			sum += uint64(b.RGB[i] - a.RGB[i])
		}
	}
	return float64(sum) / float64(len(a.RGB)), nil
}

// SharedFrame is the immutable scene origin shared by every branch. The write
// path intentionally fails: callers must use a branch-local clone for mutations.
type SharedFrame struct {
	SequenceID string
	Initial    *Frame
	Background *Frame
	SquareRect model.Rect
	BlockRect  model.Rect

	mu     sync.RWMutex
	pinned bool
}

func NewSharedFrame(seqID string, initial, background *Frame, square, block model.Rect) *SharedFrame {
	return &SharedFrame{
		SequenceID: seqID,
		Initial:    initial.Clone(),
		Background: background.Clone(),
		SquareRect: square,
		BlockRect:  block,
		pinned:     true,
	}
}

func (s *SharedFrame) InitialFrame() *Frame {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Initial.Clone()
}

func (s *SharedFrame) BackgroundFrame() *Frame {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Background.Clone()
}

func (s *SharedFrame) Texture(rect model.Rect) (*Frame, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rect.X < 0 || rect.Y < 0 || rect.W < 1 || rect.H < 1 ||
		rect.X+rect.W > s.Initial.Width || rect.Y+rect.H > s.Initial.Height {
		return nil, fmt.Errorf("invalid layer rectangle")
	}
	rgb := make([]byte, rect.W*rect.H*3)
	for y := 0; y < rect.H; y++ {
		src := ((rect.Y+y)*s.Initial.Width + rect.X) * 3
		copy(rgb[y*rect.W*3:(y+1)*rect.W*3], s.Initial.RGB[src:src+rect.W*3])
	}
	return NewFrame(rect.W, rect.H, rgb), nil
}

// Write demonstrates and enforces shared-state isolation. Even the owning
// sequence cannot mutate the pinned frame after construction.
func (s *SharedFrame) Write(sequenceID string, branchID int, offset int, data []byte) *model.AppError {
	if sequenceID == "" {
		sequenceID = s.SequenceID
	}
	err := model.NewError(model.CodeSharedFrameWrite,
		"shared initial frame is read-only; create a branch-local clone before mutating")
	err.SequenceID = sequenceID
	err.Details = map[string]any{
		"sequence_id": sequenceID,
		"branch_id":   branchID,
		"offset":      offset,
		"bytes":       len(data),
		"pinned":      s.pinned,
	}
	return model.WithBranch(err, sequenceID, branchID)
}

func SampleBilinear(tex *Frame, u, v float64) (r, g, b float64, ok bool) {
	if u < 0 || v < 0 || u > float64(tex.Width) || v > float64(tex.Height) {
		return 0, 0, 0, false
	}
	x := u - 0.5
	y := v - 0.5
	x0 := int(math.Floor(x))
	y0 := int(math.Floor(y))
	tx := x - float64(x0)
	ty := y - float64(y0)
	x1 := x0 + 1
	y1 := y0 + 1
	if x0 < 0 {
		x0 = 0
	}
	if y0 < 0 {
		y0 = 0
	}
	if x1 >= tex.Width {
		x1 = tex.Width - 1
	}
	if y1 >= tex.Height {
		y1 = tex.Height - 1
	}
	if x0 >= tex.Width || y0 >= tex.Height {
		return 0, 0, 0, false
	}
	c00 := (y0*tex.Width + x0) * 3
	c10 := (y0*tex.Width + x1) * 3
	c01 := (y1*tex.Width + x0) * 3
	c11 := (y1*tex.Width + x1) * 3
	interp := func(channel int) float64 {
		w00 := (1 - tx) * (1 - ty)
		w10 := tx * (1 - ty)
		w01 := (1 - tx) * ty
		w11 := tx * ty
		return float64(tex.RGB[c00+channel])*w00 + float64(tex.RGB[c10+channel])*w10 +
			float64(tex.RGB[c01+channel])*w01 + float64(tex.RGB[c11+channel])*w11
	}
	return interp(0), interp(1), interp(2), true
}

func blendPixel(dst []byte, off int, r, g, b, alpha float64) {
	if alpha <= 0 {
		return
	}
	if alpha > 1 {
		alpha = 1
	}
	dst[off] = model.Quantize8((float64(dst[off])/255)*(1-alpha) + r*alpha)
	dst[off+1] = model.Quantize8((float64(dst[off+1])/255)*(1-alpha) + g*alpha)
	dst[off+2] = model.Quantize8((float64(dst[off+2])/255)*(1-alpha) + b*alpha)
}

type Renderer struct{}

func (Renderer) Render(shared *SharedFrame, spec model.SequenceSpec, state model.MotionState) (*Frame, error) {
	base := shared.BackgroundFrame()
	out := append([]byte(nil), base.RGB...)
	squareTex, err := shared.Texture(shared.SquareRect)
	if err != nil {
		return nil, err
	}
	blockTex, err := shared.Texture(shared.BlockRect)
	if err != nil {
		return nil, err
	}
	drawSquare(out, spec.Width, spec.Height, squareTex, shared.SquareRect, spec.InitialState, state)
	drawBlock(out, spec.Width, spec.Height, blockTex, shared.BlockRect, spec.InitialState, state)
	return NewFrame(spec.Width, spec.Height, out), nil
}

func drawSquare(dst []byte, width, height int, tex *Frame, rect model.Rect, initial, state model.MotionState) {
	dx := state.X - initial.X
	dy := state.Y - initial.Y
	centerX := float64(rect.X) + float64(rect.W)/2 + dx
	centerY := float64(rect.Y) + float64(rect.H)/2 + dy
	minX := int(math.Floor(centerX-float64(rect.W)/2)) - 1
	maxX := int(math.Ceil(centerX+float64(rect.W)/2)) + 1
	minY := int(math.Floor(centerY-float64(rect.H)/2)) - 1
	maxY := int(math.Ceil(centerY+float64(rect.H)/2)) + 1
	for y := max(0, minY); y < min(height, maxY); y++ {
		for x := max(0, minX); x < min(width, maxX); x++ {
			u := float64(x) + 0.5 - (centerX - float64(rect.W)/2)
			v := float64(y) + 0.5 - (centerY - float64(rect.H)/2)
			if u < 0 || v < 0 || u >= float64(rect.W) || v >= float64(rect.H) {
				continue
			}
			r, g, b, ok := SampleBilinear(tex, u, v)
			if !ok {
				continue
			}
			blendPixel(dst, (y*width+x)*3, r/255, g/255, b/255, 1)
		}
	}
}

func drawBlock(dst []byte, width, height int, tex *Frame, rect model.Rect, initial, state model.MotionState) {
	dx := state.X - initial.X
	dy := state.Y - initial.Y
	scaleX := state.ScaleX / math.Max(initial.ScaleX, 0.001)
	scaleY := state.ScaleY / math.Max(initial.ScaleY, 0.001)
	scaleX = math.Max(0.55, math.Min(1.7, scaleX))
	scaleY = math.Max(0.55, math.Min(1.7, scaleY))
	rotation := state.Rotation - initial.Rotation
	centerX := float64(rect.X) + float64(rect.W)/2 + dx*0.52 + dy*0.11
	centerY := float64(rect.Y) + float64(rect.H)/2 + dy*0.46 - dx*0.07
	cosR, sinR := math.Cos(rotation), math.Sin(rotation)
	extentX := (math.Abs(cosR*float64(rect.W)*scaleX) + math.Abs(sinR*float64(rect.H)*scaleY)) / 2
	extentY := (math.Abs(sinR*float64(rect.W)*scaleX) + math.Abs(cosR*float64(rect.H)*scaleY)) / 2
	minX, maxX := int(math.Floor(centerX-extentX))-1, int(math.Ceil(centerX+extentX))+1
	minY, maxY := int(math.Floor(centerY-extentY))-1, int(math.Ceil(centerY+extentY))+1
	for y := max(0, minY); y < min(height, maxY); y++ {
		for x := max(0, minX); x < min(width, maxX); x++ {
			lx := float64(x) + 0.5 - centerX
			ly := float64(y) + 0.5 - centerY
			rx := (cosR*lx + sinR*ly) / scaleX
			ry := (-sinR*lx + cosR*ly) / scaleY
			u := rx + float64(rect.W)/2
			v := ry + float64(rect.H)/2
			if u < 0 || v < 0 || u >= float64(rect.W) || v >= float64(rect.H) {
				continue
			}
			r, g, b, ok := SampleBilinear(tex, u, v)
			if !ok {
				continue
			}
			blendPixel(dst, (y*width+x)*3, r/255, g/255, b/255, 0.94)
		}
	}
}
