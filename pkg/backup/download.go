package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"
	legacyStorage "github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/utils"

	apexLog "github.com/apex/log"
)
var (
	ErrBackupIsAlreadyExists = errors.New("backup is already exists")
)

func legacyDownload(cfg *config.Config, defaultDataPath, backupName string) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	bd, err := legacyStorage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	if err := bd.Connect(); err != nil {
		return err
	}
	if err := bd.CompressedStreamDownload(backupName,
		path.Join(defaultDataPath, "backup", backupName)); err != nil {
		return err
	}
	log.Info("done")
	return nil
}

func (b *Backuper) Download(backupName string, tablePattern string, schemaOnly bool) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "download",
	})
	if b.cfg.General.RemoteStorage == "none" {
		return fmt.Errorf("remote storage is 'none'")
	}
	if backupName == "" {
		PrintRemoteBackups(b.cfg, "all")
		return fmt.Errorf("select backup for download")
	}
	localBackups, err := GetLocalBackups(b.cfg)
	if err != nil {
		return err
	}
	for i := range localBackups {
		if backupName == localBackups[i].BackupName {
			return ErrBackupIsAlreadyExists
		}
	}
	startDownload := time.Now()
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if err := b.init(); err != nil {
		return err
	}

	// fix for gcs
	remoteBackups, err := b.dst.BackupFolderList(backupName, b.cfg.General.RemoteStorage)
	if err != nil {
		return err
	}
	found := false
	var remoteBackup new_storage.Backup
	for _, b := range remoteBackups {
		if backupName == b.BackupName {
			remoteBackup = b
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("'%s' is not found on remote storage", backupName)
	}
	if len(remoteBackup.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return fmt.Errorf("'%s' is empty backup", backupName)
	}
	if remoteBackup.Legacy {
		if tablePattern != "" {
			return fmt.Errorf("'%s' is old format backup and doesn't supports download of specific tables", backupName)
		}
		if schemaOnly {
			return fmt.Errorf("'%s' is old format backup and doesn't supports download of schema only", backupName)
		}
		log.Debugf("'%s' is old-format backup", backupName)
		return legacyDownload(b.cfg, b.DefaultDataPath, backupName)
	}
	tableMetadataForDownload := []metadata.TableMetadata{}
	tablesForDownload := parseTablePatternForDownload(remoteBackup.Tables, tablePattern)

	if !schemaOnly && remoteBackup.RequiredBackup != "" {
		err := b.Download(remoteBackup.RequiredBackup, tablePattern, schemaOnly)
		if err != nil && err != ErrBackupIsAlreadyExists {
			return err
		}
	}

	dataSize := int64(0)
	metadataSize := int64(0)
	err = os.MkdirAll(path.Join(b.DefaultDataPath, "backup", backupName), 0750)
	if err != nil {
		return err
	}
	for _, t := range tablesForDownload {
		log := log.WithField("table", fmt.Sprintf("%s.%s", t.Database, t.Table))
		remoteTableMetadata := path.Join(backupName, "metadata", clickhouse.TablePathEncode(t.Database), fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		log.Debug(remoteTableMetadata)
		tmReader, err := b.dst.GetFileReader(remoteTableMetadata)
		if err != nil {
			return err
		}
		tmBody, err := ioutil.ReadAll(tmReader)
		if err != nil {
			return err
		}
		tmReader.Close()
		var tableMetadata metadata.TableMetadata
		if err := json.Unmarshal(tmBody, &tableMetadata); err != nil {
			return err
		}
		tableMetadataForDownload = append(tableMetadataForDownload, tableMetadata)

		// save metadata
		metadataLocalFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata", clickhouse.TablePathEncode(t.Database), fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		size, err := tableMetadata.Save(metadataLocalFile, schemaOnly)
		if err != nil {
			return err
		}
		metadataSize += int64(size)
		log.Info("done")
	}

	if !schemaOnly {
		for _, t := range tableMetadataForDownload {
			for disk := range t.Parts {
				if _, ok := b.DiskMap[disk]; !ok {
					return fmt.Errorf("table '%s.%s' require disk '%s' that not found in clickhouse, you can add nonexistent disks to disk_mapping config", t.Database, t.Table, disk)
				}
			}
		}
		for _, tableMetadata := range tableMetadataForDownload {
			if tableMetadata.MetadataOnly {
				continue
			}
			dataSize += tableMetadata.TotalBytes
			start := time.Now()
			if err := b.downloadTableData(remoteBackup.BackupMetadata, tableMetadata); err != nil {
				return err
			}
			log.
				WithField("table", fmt.Sprintf("%s.%s", tableMetadata.Database, tableMetadata.Table)).
				WithField("duration", utils.HumanizeDuration(time.Since(start))).
				WithField("size", utils.FormatBytes(metadataSize+tableMetadata.TotalBytes)).
				Info("done")
		}
	}
	backupMetadata := remoteBackup.BackupMetadata
	backupMetadata.Tables = tablesForDownload
	backupMetadata.DataSize = dataSize
	backupMetadata.MetadataSize = metadataSize
	backupMetadata.CompressedSize = 0
	backupMetadata.DataFormat = ""
	backupMetadata.RequiredBackup = ""

	backupMetafileLocalPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	if err := backupMetadata.Save(backupMetafileLocalPath); err != nil {
		return err
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startDownload))).
		WithField("size", utils.FormatBytes(dataSize+metadataSize)).
		Info("done")
	return nil
}

func (b *Backuper) downloadTableData(remoteBackup metadata.BackupMetadata, table metadata.TableMetadata) error {
	uuid := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
	if remoteBackup.DataFormat != "directory" {
		for disk := range table.Files {
			diskPath := b.DiskMap[disk]
			tableLocalDir := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", uuid, disk)
			for _, archiveFile := range table.Files[disk] {
				tableRemoteFile := path.Join(remoteBackup.BackupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table), archiveFile)
				if err := b.dst.CompressedStreamDownload(tableRemoteFile, tableLocalDir); err != nil {
					return err
				}
			}
		}
	} else {
		for disk := range table.Parts {
			tableRemotePath := path.Join(remoteBackup.BackupName, "shadow", uuid, disk)
			diskPath := b.DiskMap[disk]
			tableLocalDir := path.Join(diskPath, "backup", remoteBackup.BackupName, "shadow", uuid, disk)
			if err := b.dst.DownloadPath(0, tableRemotePath, tableLocalDir); err != nil {
				return err
			}
		}
	}
	// Create symlink for exsists parts
	for disk, parts := range table.Parts {
		for _, p := range parts {
			if !p.Required {
				continue
			}
			existsPath := path.Join(b.DiskMap[disk], "backup", remoteBackup.RequiredBackup, "shadow", uuid, disk, p.Name)
			newPath := path.Join(b.DiskMap[disk], "backup", remoteBackup.BackupName, "shadow", uuid, disk, p.Name)
			if err := duplicatePart(existsPath, newPath); err != nil {
				return fmt.Errorf("can't to add exists part: %s", err)
			}
		}
	}
	return nil
}

func duplicatePart(exists, new string) error {
	ex, err := os.Open(exists)
	if err != nil {
		return err
	}
	defer ex.Close()
	files, err := ex.Readdirnames(-1)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(new, 0750); err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Link(path.Join(exists, f), path.Join(new, f)); err != nil {
			return err
		}
	}
	return nil
}
