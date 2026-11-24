package model

import (
	"fmt"
	"os"
	"path/filepath"
)

type Samples struct {
	Cluster     *ClusterSpec
	Policy      *Policy
	Values      *DriftValuesSample
	ObjectsPath string
	PolicyPath  string
	ValuesPath  string
}

func LoadSamples(objectsPath, policyPath, valuesPath string) (*Samples, error) {
	clusterBytes, err := readSample(objectsPath)
	if err != nil {
		return nil, invalid("cannot read cluster object sample: %v", err)
	}
	policyBytes, err := readSample(policyPath)
	if err != nil {
		return nil, invalid("cannot read policy snapshot: %v", err)
	}
	valuesBytes, err := readSample(valuesPath)
	if err != nil {
		return nil, invalid("cannot read drift field values sample: %v", err)
	}
	policy, err := DecodePolicy(policyBytes)
	if err != nil {
		return nil, err
	}
	cluster, err := DecodeCluster(clusterBytes)
	if err != nil {
		return nil, err
	}
	if err := policy.ValidateAgainstCluster(cluster); err != nil {
		return nil, err
	}
	values, err := DecodeValues(valuesBytes)
	if err != nil {
		return nil, err
	}
	return &Samples{
		Cluster:     cluster,
		Policy:      policy,
		Values:      values,
		ObjectsPath: absoluteOrClean(objectsPath),
		PolicyPath:  absoluteOrClean(policyPath),
		ValuesPath:  absoluteOrClean(valuesPath),
	}, nil
}

func readSample(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("path is empty")
	}
	return os.ReadFile(path)
}

func absoluteOrClean(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}
