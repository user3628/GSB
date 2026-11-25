package pipeline

// SceneFingerprints bundles the content fingerprints used for change
// detection and cache keys (requirement 4, 6).
type SceneFingerprints struct {
	// CameraFP fingerprints every extrinsic/intrinsic across the scene.
	CameraFP string `json:"camera_fp"`
	// ContentFP fingerprints every image tile (pixels + NaN mask).
	ContentFP string `json:"content_fp"`
	// PerViewImageFP allows unchanged views to keep their local artifacts.
	PerViewImageFP map[string]string `json:"per_view_image_fp"`
	// PerViewCameraFP allows unchanged cameras to be reused.
	PerViewCameraFP map[string]string `json:"per_view_camera_fp"`
}

// ComputeFingerprints derives all scene fingerprints deterministically.
func ComputeFingerprints(m *Manifest, images map[string]*ViewImage) (SceneFingerprints, error) {
	fp := SceneFingerprints{
		PerViewImageFP:  map[string]string{},
		PerViewCameraFP: map[string]string{},
	}
	type camEntry struct {
		ShardID string
		Camera  Camera
	}
	var cams []camEntry
	var imageFP []struct {
		ViewID string
		Digest string
	}
	for _, shard := range m.Shards {
		for _, cam := range shard.Views {
			camFP, err := Digest(cam)
			if err != nil {
				return fp, err
			}
			fp.PerViewCameraFP[cam.ViewID] = camFP
			cams = append(cams, camEntry{shard.ShardID, cam})
			img, ok := images[cam.ViewID]
			if !ok {
				return fp, &BizError{Code: CodeNotFound, Message: "missing tile",
					ShardID: shard.ShardID, ViewID: cam.ViewID}
			}
			d := img.ContentDigest()
			fp.PerViewImageFP[cam.ViewID] = d
			imageFP = append(imageFP, struct {
				ViewID string
				Digest string
			}{cam.ViewID, d})
		}
	}
	var err error
	fp.CameraFP, err = Digest(cams)
	if err != nil {
		return fp, err
	}
	fp.ContentFP, err = Digest(imageFP)
	if err != nil {
		return fp, err
	}
	return fp, nil
}

// StageInputDigest identifies the exact logical input of a stage for one view.
// Projection depends on pixels + camera + config. Local volume depends on the
// projection digest + voxel/truncation config. The chain is encoded explicitly
// so the earliest affected stage is detectable.
func projectionInputDigest(viewID string, fp SceneFingerprints, cfgVer string) string {
	return MustDigest(struct {
		Stage   string
		ViewID  string
		ImageFP string
		CamFP   string
		CfgVer  string
	}{StageProjection, viewID, fp.PerViewImageFP[viewID], fp.PerViewCameraFP[viewID], cfgVer})
}

func localInputDigest(viewID, projDigest string, cfg PipelineConfig) string {
	return MustDigest(struct {
		Stage         string
		ViewID        string
		ProjDigest    string
		Voxel         float64
		Truncation    float64
		SampleDensity float64
	}{StageLocal, viewID, projDigest, cfg.VoxelResolution, cfg.Truncation, cfg.SampleDensity})
}
