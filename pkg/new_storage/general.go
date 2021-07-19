package new_storage

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/internal/progressbar"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"golang.org/x/sync/errgroup"

	apexLog "github.com/apex/log"
	"github.com/mholt/archiver/v3"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
)

const (
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 4 * 1024 * 1024
)

type Backup struct {
	metadata.BackupMetadata
	Legacy        bool
	FileExtension string
	Broken        string
	UploadDate    time.Time
}

type BackupDestination struct {
	RemoteStorage
	compressionFormat  string
	compressionLevel   int
	disableProgressBar bool
}

func (bd *BackupDestination) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backupToDelete := range backupsToDelete {
		if err := bd.RemoveBackup(backupToDelete); err != nil {
			return err
		}
		apexLog.WithField("operation", "delete").
			WithField("location", "remote").
			WithField("backup", backupToDelete.BackupName).
			Info("done")
	}
	return nil
}

func (bd *BackupDestination) RemoveBackup(backup Backup) error {
	if bd.Kind() == "SFTP" {
		return bd.DeleteFile(backup.BackupName)
	}
	if backup.Legacy {
		archiveName := fmt.Sprintf("%s.%s", backup.BackupName, backup.FileExtension)
		return bd.DeleteFile(archiveName)
	}
	return bd.Walk(backup.BackupName+"/", true, func(f RemoteFile) error {
		return bd.DeleteFile(path.Join(backup.BackupName, f.Name()))
	})
}

func isLegacyBackup(backupName string) (bool, string, string) {
	for _, suffix := range config.ArchiveExtensions {
		if strings.HasSuffix(backupName, "."+suffix) {
			return true, strings.TrimSuffix(backupName, "."+suffix), suffix
		}
	}
	return false, backupName, ""
}

func (bd *BackupDestination) BackupList() ([]Backup, error) {
	result := []Backup{}
	err := bd.Walk("/", false, func(o RemoteFile) error {
		// Legacy backup
		if ok, backupName, fileExtension := isLegacyBackup(strings.TrimPrefix(o.Name(), "/")); ok {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
					DataSize:   o.Size(),
				},
				true,
				fileExtension,
				"",
				o.LastModified(),
			})
			return nil
		}
		mf, err := bd.StatFile(path.Join(o.Name(), "metadata.json"))
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (can't stat metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		r, err := bd.GetFileReader(path.Join(o.Name(), "metadata.json"))
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (not found metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		b, err := ioutil.ReadAll(r)
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (can't get metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		if err := r.Close(); err != nil { // Never use defer in loops
			return err
		}
		var m metadata.BackupMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (bad metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		result = append(result, Backup{
			m, false, "", "", mf.LastModified(),
		})
		return nil
	})
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UploadDate.Before(result[j].UploadDate)
	})
	return result, err
}

// use this function only for gcs
func (bd *BackupDestination) BackupFolderList(backupFolderName string, remoteStorage string) ([]Backup, error) {
	if remoteStorage != "gcs" {
		return bd.BackupList()
	}
	result := []Backup{}
	err := bd.Walk(backupFolderName, false, func(o RemoteFile) error {
		// Legacy backup
		if ok, backupName, fileExtension := isLegacyBackup(strings.TrimPrefix(o.Name(), "/")); ok {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
					DataSize:   o.Size(),
				},
				true,
				fileExtension,
				"",
				o.LastModified(),
			})
			return nil
		}
		mf, err := bd.StatFile(path.Join(o.Name(), "metadata.json"))
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (can't stat metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		r, err := bd.GetFileReader(path.Join(o.Name(), "metadata.json"))
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (not found metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		b, err := ioutil.ReadAll(r)
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (can't get metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		if err := r.Close(); err != nil { // Never use defer in loops
			return err
		}
		var m metadata.BackupMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				},
				false,
				"",
				"broken (bad metadata.json)",
				o.LastModified(), // folder
			})
			return nil
		}
		result = append(result, Backup{
			m, false, "", "", mf.LastModified(),
		})
		return nil
	})
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UploadDate.Before(result[j].UploadDate)
	})
	return result, err
}

func (bd *BackupDestination) CompressedStreamDownload(remotePath string, localPath string) error {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return err
	}
	// get this first as GetFileReader blocks the ftp control channel
	file, err := bd.StatFile(remotePath)
	if err != nil {
		return err
	}
	filesize := file.Size()

	reader, err := bd.GetFileReader(remotePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	defer bar.Finish()
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, err := getArchiveReader(bd.compressionFormat)
	if err != nil {
		return err
	}
	if err := z.Open(proxyReader, 0); err != nil {
		return err
	}
	defer z.Close()
	for {
		file, err := z.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		header, ok := file.Header.(*tar.Header)
		if !ok {
			return fmt.Errorf("expected header to be *tar.Header but was %T", file.Header)
		}
		extractFile := filepath.Join(localPath, header.Name)
		extractDir := filepath.Dir(extractFile)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			os.MkdirAll(extractDir, 0750)
		}
		dst, err := os.Create(extractFile)
		if err != nil {
			return err
		}
		if _, err := io.Copy(dst, file); err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) CompressedStreamUpload(baseLocalPath string, files []string, remotePath string) error {
	if _, err := bd.StatFile(remotePath); err != nil {
		if err != ErrNotFound && !os.IsNotExist(err) {
			return err
		}
	}
	var totalBytes int64
	for _, filename := range files {
		finfo, err := os.Stat(path.Join(baseLocalPath, filename))
		if err != nil {
			return err
		}
		if finfo.Mode().IsRegular() {
			totalBytes += finfo.Size()
		}
	}
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()
	buf := buffer.New(BufferSize)
	body, w := nio.Pipe(buf)
	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer w.Close()
		iobuf := buffer.New(BufferSize)
		z, _ := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if err := z.Create(w); err != nil {
			return err
		}
		defer z.Close()
		for _, f := range files {
			filePath := path.Join(baseLocalPath, f)
			info, err := os.Stat(filePath)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			bar.Add64(info.Size())
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			bfile := nio.NewReader(file, iobuf)
			if err := z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: f,
				},
				ReadCloser: bfile,
			}); err != nil {
				return err
			}
			if err := bfile.Close(); err != nil { // No use defer for this
				return err
			}
			if err := file.Close(); err != nil { // No use defer for this too
				return err
			}
		}
		return nil
	})
	g.Go(func() error {
		return bd.PutFile(remotePath, body)
	})
	return g.Wait()
}

func (bd *BackupDestination) DownloadPath(size int64, remotePath string, localPath string) error {
	totalBytes := size
	if size == 0 {
		if err := bd.Walk(remotePath, true, func(f RemoteFile) error {
			totalBytes += f.Size()
			return nil
		}); err != nil {
			return err
		}
	}
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()
	log := apexLog.WithFields(apexLog.Fields{
		"path":      remotePath,
		"operation": "download",
	})
	return bd.Walk(remotePath, true, func(f RemoteFile) error {
		// TODO: return err приостанавливает загрузку, нужно научить Walk обратывать ошибки или добавить какие-то ретраи
		r, err := bd.GetFileReader(path.Join(remotePath, f.Name()))
		if err != nil {
			log.Error(err.Error())
			return err
		}
		dstFilePath := path.Join(localPath, f.Name())
		dstDirPath, _ := path.Split(dstFilePath)
		if err := os.MkdirAll(dstDirPath, 0750); err != nil {
			log.Error(err.Error())
			return err
		}
		dst, err := os.Create(dstFilePath)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		if _, err := io.CopyBuffer(dst, r, nil); err != nil {
			log.Error(err.Error())
			return err
		}
		if err := dst.Close(); err != nil {
			log.Error(err.Error())
			return err
		}
		if err := r.Close(); err != nil {
			log.Error(err.Error())
			return err
		}
		bar.Add64(f.Size())
		return nil
	})
}

func (bd *BackupDestination) UploadPath(size int64, baseLocalPath string, files []string, remotePath string) error {
	totalBytes := size
	if size == 0 {
		for _, filename := range files {
			finfo, err := os.Stat(path.Join(baseLocalPath, filename))
			if err != nil {
				return err
			}
			if finfo.Mode().IsRegular() {
				totalBytes += finfo.Size()
			}
		}
	}
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()

	for _, filename := range files {
		f, err := os.Open(path.Join(baseLocalPath, filename))
		if err != nil {
			return err
		}
		if err := bd.PutFile(path.Join(remotePath, filename), f); err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		bar.Add64(fi.Size())
		f.Close()
	}

	return nil
}

func NewBackupDestination(cfg *config.Config) (*BackupDestination, error) {
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{Config: &cfg.AzureBlob}
		return &BackupDestination{
			azblobStorage,
			cfg.AzureBlob.CompressionFormat,
			cfg.AzureBlob.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "s3":
		s3Storage := &S3{
			Config:      &cfg.S3,
			Concurrency: 1,
			BufferSize:  1024 * 1024,
		}
		return &BackupDestination{
			s3Storage,
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		return &BackupDestination{
			googleCloudStorage,
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "cos":
		tencentStorage := &COS{
			Config: &cfg.COS,
			Debug:  cfg.General.LogLevel == "debug",
		}
		return &BackupDestination{
			tencentStorage,
			cfg.COS.CompressionFormat,
			cfg.COS.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "ftp":
		ftpStorage := &FTP{
			Config: &cfg.FTP,
			Debug:  cfg.General.LogLevel == "debug",
		}
		return &BackupDestination{
			ftpStorage,
			cfg.FTP.CompressionFormat,
			cfg.FTP.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "sftp":
		sftpStorage := &SFTP{
			Config: &cfg.SFTP,
		}
		return &BackupDestination{
			sftpStorage,
			cfg.SFTP.CompressionFormat,
			cfg.SFTP.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	default:
		return nil, fmt.Errorf("storage type '%s' is not supported", cfg.General.RemoteStorage)
	}
}
