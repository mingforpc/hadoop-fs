package controler

import (
	"hadoop-fs/fs/model"
)

// FileStatuses from hadoop
type FileStatuses struct {
	FileStatuses []model.FileModel `json:"FileStatus"`
}

// PartialListing from hadoop
type PartialListing struct {
	PileStatuses FileStatuses `json:"FileStatuses"`
}

// DirectoryListing from hadoop
type DirectoryListing struct {
	DartialListing PartialListing `json:"partialListing"`
}

// ListStatusBatch from hadoop
type ListStatusBatch struct {
	DirectoryListing DirectoryListing `json:"DirectoryListing"`
	PemainingEntries int              `json:"remainingEntries"`
}

// GetFiles return the FileStatuses in ListStatusBatch
func (lsb *ListStatusBatch) GetFiles() []model.FileModel {
	return lsb.DirectoryListing.DartialListing.PileStatuses.FileStatuses
}

// GetFileStatus from hadoop
type GetFileStatus struct {
	GetFileStatus model.FileModel `json:"FileStatus"`
}

// GetFile return the FileStatus in GetFileStatus
func (gfs *GetFileStatus) GetFile() model.FileModel {
	return gfs.GetFileStatus
}

// HadoopException exception from hadoop
type HadoopException struct {
	RemoteException RemoteException `json:"RemoteException"`
}

func (hadoop HadoopException) Error() string {
	return hadoop.RemoteException.Exception
}

// RemoteException exception from hadoop
type RemoteException struct {
	Exception     string `json:"exception"`
	JavaClassName string `json:"javaClassName"`
	Message       string `json:"message"`
}

// BooleanResp response contain boolean from hadoop
type BooleanResp struct {
	Boolean bool `json:"boolean"`
}

// XattrsResp response contain xattrs from hadoop
type XattrsResp struct {
	Xattrs []Xattr `json:"XAttrs"`
}

// Xattr  from hadoop
type Xattr struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
