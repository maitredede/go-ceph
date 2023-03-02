//go:build ceph_preview
// +build ceph_preview

package cephfs

/*
#cgo LDFLAGS: -lcephfs
#cgo CPPFLAGS: -D_FILE_OFFSET_BITS=64
#include <stdlib.h>
#include <cephfs/libcephfs.h>

struct snap_metadata getSnapMeta(struct snap_info info, int index) {
	return info.snap_metadata[index];
}

struct snap_metadata* initMeta(const ulong nr) {
	return (struct snap_metadata*)malloc(sizeof(struct snap_metadata) * nr);
}

void setMeta(struct snap_metadata *p, int i, const char *key, const char* value) {
	p[i].key = key;
	p[i].value = value;
}

void freeMeta(struct snap_metadata *p) {
	free(p);
}
*/
import "C"

import (
	"unsafe"

	intLog "github.com/ceph/go-ceph/internal/log"
)

// MakeSnap create a snapshot.
func (mount *MountInfo) MakeSnap(path string, name string, mode uint32, metadata map[string]string) error {
	if err := mount.validate(); err != nil {
		return err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cMode := C.mode_t(mode)

	nr := len(metadata)
	if nr == 0 {
		ret := C.ceph_mksnap(mount.mount, cPath, cName, cMode, nil, 0)
		return getError(ret)
	}

	metaArr := make([]C.struct_snap_metadata, nr)
	i := 0
	for k, v := range metadata {
		metaArr[i].key = C.CString(k)
		metaArr[i].value = C.CString(v)
		i++
	}
	defer func() {
		for i := 0; i < nr; i++ {
			C.free(unsafe.Pointer(metaArr[i].key))
			C.free(unsafe.Pointer(metaArr[i].value))
		}
	}()
	pMeta := (*C.struct_snap_metadata)(&metaArr[0])
	intLog.Debugf("mksnap path=%v", path)
	intLog.Debugf("mksnap name=%v", name)
	intLog.Debugf("mksnap mode=%O", mode)
	intLog.Debugf("mksnap meta=%+v", metadata)
	ret := C.ceph_mksnap(mount.mount, cPath, cName, cMode, pMeta, C.ulong(nr))
	return getError(ret)
}

// RemoveSnap remove a snapshot.
func (mount *MountInfo) RemoveSnap(path string, name string) error {
	if err := mount.validate(); err != nil {
		return err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.ceph_rmsnap(mount.mount, cPath, cName)
	return getError(ret)
}

type SnapInfo struct {
	ID       uint64
	Metadata map[string]string
}

// GetSnapInfo Fetch snapshot info.
func (mount *MountInfo) GetSnapInfo(path string) (*SnapInfo, error) {
	if err := mount.validate(); err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cSnapInfo C.struct_snap_info
	ret := C.ceph_get_snap_info(mount.mount, cPath, &cSnapInfo)
	if err := getError(ret); err != nil {
		return nil, err
	}

	result := SnapInfo{
		ID:       uint64(cSnapInfo.id),
		Metadata: make(map[string]string),
	}
	if cSnapInfo.nr_snap_metadata > 0 {
		defer C.ceph_free_snap_info_buffer(&cSnapInfo)
		for i := uint64(0); i < uint64(cSnapInfo.nr_snap_metadata); i++ {
			cItem := C.getSnapMeta(cSnapInfo, C.int(i))
			key := C.GoString(cItem.key)
			value := C.GoString(cItem.value)
			result.Metadata[key] = value
		}
	}
	return &result, nil
}
