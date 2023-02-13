//go:build ceph_preview
// +build ceph_preview

package cephfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateRemoveSnap(t *testing.T) {
	mount := fsConnect(t)
	defer fsDisconnect(t, mount)

	dir1 := "/asdf"
	err := mount.MakeDir(dir1, 0755)
	assert.NoError(t, err)

	snapName := "hello-snap"
	err = mount.MakeSnap(dir1, snapName, 0755, nil)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, mount.RemoveDir(dir1)) }()

	err = mount.RemoveSnap(dir1, snapName)
	assert.NoError(t, err)
}

func TestCreateRemoveSnapWithMeta(t *testing.T) {
	mount := fsConnect(t)
	defer fsDisconnect(t, mount)

	dir1 := "/asdf"
	err := mount.MakeDir(dir1, 0755)
	assert.NoError(t, err)

	meta := map[string]string{
		"testName": t.Name(),
	}
	snapName := "hello-snap"
	err = mount.MakeSnap(dir1, snapName, 0755, meta)
	assert.NoError(t, err)

	snapInfo, err := mount.GetSnapInfo(dir1)
	if assert.NoError(t, err) {
		assert.NotNil(t, snapInfo)
		assert.Equal(t, len(meta), len(snapInfo.Metadata))
	}
}
