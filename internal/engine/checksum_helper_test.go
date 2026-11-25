package engine

import "alarmengine/internal/model"
import "alarmengine/internal/sample"

func checksumOf(p model.PolicySnapshot) string { return sample.ComputeChecksum(p) }
