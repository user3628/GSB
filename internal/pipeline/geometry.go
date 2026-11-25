package pipeline

import "math"

func matMul(a, b Mat3) Mat3 {
	var r Mat3
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			r[i*3+j] = a[i*3+0]*b[0*3+j] + a[i*3+1]*b[1*3+j] + a[i*3+2]*b[2*3+j]
		}
	}
	return r
}

func matVec(a Mat3, v Vec3) Vec3 {
	return Vec3{
		a[0]*v[0] + a[1]*v[1] + a[2]*v[2],
		a[3]*v[0] + a[4]*v[1] + a[5]*v[2],
		a[6]*v[0] + a[7]*v[1] + a[8]*v[2],
	}
}

func transpose(a Mat3) Mat3 {
	return Mat3{
		a[0], a[3], a[6],
		a[1], a[4], a[7],
		a[2], a[5], a[8],
	}
}

func vecSub(a, b Vec3) Vec3 { return Vec3{a[0] - b[0], a[1] - b[1], a[2] - b[2]} }
func vecAdd(a, b Vec3) Vec3 { return Vec3{a[0] + b[0], a[1] + b[1], a[2] + b[2]} }
func vecScale(a Vec3, s float64) Vec3 {
	return Vec3{a[0] * s, a[1] * s, a[2] * s}
}
func vecDot(a, b Vec3) float64 { return a[0]*b[0] + a[1]*b[1] + a[2]*b[2] }
func vecNorm(a Vec3) float64   { return math.Sqrt(vecDot(a, a)) }

// orthonormalTolerance is the maximum deviation of R*R^T from identity and
// |det(R)-1| accepted as a valid rotation. It is a fixed constant so the
// validity decision is reproducible.
const orthonormalTolerance = 1e-9

// validateRotation rejects non-orthogonal pose matrices (requirement 1). The
// returned field name is reported in the structured business error.
func validateRotation(r Mat3) (string, bool) {
	rr := matMul(r, transpose(r))
	var id Mat3
	id[0], id[4], id[8] = 1, 1, 1
	for i := 0; i < 9; i++ {
		if math.Abs(rr[i]-id[i]) > orthonormalTolerance {
			return "r", false
		}
	}
	det := r[0]*(r[4]*r[8]-r[5]*r[7]) -
		r[1]*(r[3]*r[8]-r[5]*r[6]) +
		r[2]*(r[3]*r[7]-r[4]*r[6])
	if math.Abs(det-1) > orthonormalTolerance {
		return "r", false
	}
	return "", true
}

func validateIntrinsics(k Intrinsics) (string, bool) {
	if k.FX == 0 || k.FY == 0 {
		return "k.focal", false
	}
	if math.IsNaN(k.FX) || math.IsNaN(k.FY) || math.IsInf(k.FX, 0) || math.IsInf(k.FY, 0) {
		return "k.focal", false
	}
	return "", true
}

// cameraRay maps a pixel to a world-space ray (origin, unit direction).
// Xc = R Xw + T  =>  Xw = R^T (Xc - T).
func cameraRay(cam Camera, px, py int) (Vec3, Vec3) {
	xc := Vec3{
		(float64(px) - cam.K.CX) / cam.K.FX,
		(float64(py) - cam.K.CY) / cam.K.FY,
		1,
	}
	rt := transpose(cam.R)
	dir := matVec(rt, xc)
	n := vecNorm(dir)
	dir = vecScale(dir, 1/n)
	origin := matVec(rt, vecScale(cam.T, -1))
	return origin, dir
}

// projectWorld projects a world point back into camera pixel coordinates.
func projectWorld(cam Camera, p Vec3) (float64, float64, float64) {
	xc := vecAdd(matVec(cam.R, p), cam.T)
	u := cam.K.FX*xc[0]/xc[2] + cam.K.CX
	v := cam.K.FY*xc[1]/xc[2] + cam.K.CY
	return u, v, xc[2]
}

// quantize maps a world coordinate to an integer voxel coordinate. Negative
// coordinates use floor semantics so neighbouring points never straddle a
// grid boundary differently.
func quantize(p Vec3, resolution float64) VoxelKey {
	return VoxelKey{
		int32(math.Floor(p[0] / resolution)),
		int32(math.Floor(p[1] / resolution)),
		int32(math.Floor(p[2] / resolution)),
	}
}

// voxelCenter returns the deterministic world-space center of a voxel.
func voxelCenter(k VoxelKey, resolution float64) Vec3 {
	return Vec3{
		(float64(k[0]) + 0.5) * resolution,
		(float64(k[1]) + 0.5) * resolution,
		(float64(k[2]) + 0.5) * resolution,
	}
}

func absInt32(a int32) int32 {
	if a < 0 {
		return -a
	}
	return a
}

// chebyshev is the Chebyshev distance used by the fusion-radius weighting.
func chebyshev(a, b VoxelKey) int32 {
	dx := absInt32(a[0] - b[0])
	dy := absInt32(a[1] - b[1])
	dz := absInt32(a[2] - b[2])
	m := dx
	if dy > m {
		m = dy
	}
	if dz > m {
		m = dz
	}
	return m
}

func arccos(x float64) float64 { return math.Acos(x) }
