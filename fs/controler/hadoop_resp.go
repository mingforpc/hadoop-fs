package controler

import (
	"hadoop-fs/fs/model"
)

type FileStatuses struct {
	FileStatuses []model.FileModel `json:"FileStatus"`
}

type PartialListing struct {
	PileStatuses FileStatuses `json:"FileStatuses"`
}

type DirectoryListing struct {
	DartialListing PartialListing `json:"partialListing"`
}

type ListStatusBatch struct {
	DirectoryListing DirectoryListing `json:"DirectoryListing"`
	PemainingEntries int              `json:"remainingEntries"`
}

func (lsb *ListStatusBatch) GetFiles() []model.FileModel {
	return lsb.DirectoryListing.DartialListing.PileStatuses.FileStatuses
}

type GetFileStatus struct {
	GetFileStatus model.FileModel `json:"FileStatus"`
}

func (gfs *GetFileStatus) GetFile() model.FileModel {
	return gfs.GetFileStatus
}

type HadoopException struct {
	RemoteException RemoteException `json:"RemoteException"`
}

func (hadoop HadoopException) Error() string {
	return hadoop.RemoteException.Exception
}

type RemoteException struct {
	Exception     string `json:"exception"`
	JavaClassName string `json:"javaClassName"`
	Message       string `json:"message"`
}

type BooleanResp struct {
	Boolean bool `json:"boolean"`
}
