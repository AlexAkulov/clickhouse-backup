package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/utils"
	apexLog "github.com/apex/log"
	"github.com/yargevad/filepathx"
)

func (b *Backuper) Upload(backupName string, tablePattern string, diffFrom string, schemaOnly bool) error {
	if b.cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		_ = PrintLocalBackups(b.cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	if backupName == diffFrom {
		return fmt.Errorf("you cannot upload diff from the same backup")
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "upload",
	})
	startUpload := time.Now()
	if err := b.ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer b.ch.Close()
	if err := b.init(); err != nil {
		return err
	}
	if _, err := getLocalBackup(b.cfg, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	remoteBackups, err := b.dst.BackupList()
	if err != nil {
		return err
	}
	for i := range remoteBackups {
		if backupName == remoteBackups[i].BackupName {
			return fmt.Errorf("'%s' already exists on remote", backupName)
		}
	}
	backupMetadata, err := b.ReadBackupMetadata(backupName)
	if err != nil {
		return err
	}
	var tablesForUpload RestoreTables
	if len(backupMetadata.Tables) != 0 {
		metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
		tablesForUpload, err = parseSchemaPattern(metadataPath, tablePattern, false)
		if err != nil {
			return err
		}
	}
	compressedDataSize := int64(0)
	metadataSize := int64(0)
	var diffFromBackup *metadata.BackupMetadata
	tablesForUploadFromDiff := map[metadata.TableTitle]metadata.TableMetadata{}
	if diffFrom != "" {
		diffFromBackup, err = b.ReadBackupMetadata(diffFrom)
		if err != nil {
			return err
		}
		if len(diffFromBackup.Tables) != 0 {
			backupMetadata.RequiredBackup = diffFrom
			metadataPath := path.Join(b.DefaultDataPath, "backup", diffFrom, "metadata")
			diffTablesList, err := parseSchemaPattern(metadataPath, tablePattern, false)
			if err != nil {
				return err
			}
			for _, t := range diffTablesList {
				tablesForUploadFromDiff[metadata.TableTitle{
					Database: t.Database,
					Table:    t.Table,
				}] = t
			}
		}
	}
	for _, table := range tablesForUpload {
		// if table.UUID != "" {
		// 	uuid = path.Join(table.UUID[0:3], table.UUID)
		// }
		start := time.Now()
		var uploadedBytes int64
		if !schemaOnly {
			if diffTable, ok := tablesForUploadFromDiff[metadata.TableTitle{
				Database: table.Database,
				Table:    table.Table,
			}]; ok {
				b.markDuplicatedParts(backupMetadata, &diffTable, &table)
			}
			var files map[string][]string
			files, uploadedBytes, err = b.uploadTableData(backupName, table)
			if err != nil {
				return err
			}
			compressedDataSize += uploadedBytes
			table.Files = files
		}
		tableMetadataSize, err := b.uploadTableMetadata(backupName, table)
		if err != nil {
			return err
		}
		metadataSize += tableMetadataSize
		log.
			WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).
			WithField("duration", utils.HumanizeDuration(time.Since(start))).
			WithField("size", utils.FormatBytes(uploadedBytes+tableMetadataSize)).
			Info("done")
	}
	// upload rbac for backup
	if backupMetadata.RBACSize, err = b.uploadRBACData(backupName); err != nil {
		return err
	}

	// upload configs for backup
	if backupMetadata.ConfigSize, err = b.uploadConfigData(backupName); err != nil {
		return err
	}

	// upload metadata for backup
	backupMetadata.CompressedSize = compressedDataSize
	backupMetadata.MetadataSize = metadataSize
	tt := []metadata.TableTitle{}
	for i := range tablesForUpload {
		tt = append(tt, metadata.TableTitle{
			Database: tablesForUpload[i].Database,
			Table:    tablesForUpload[i].Table,
		})
	}
	backupMetadata.Tables = tt
	if b.cfg.GetCompressionFormat() != "none" {
		backupMetadata.DataFormat = b.cfg.GetCompressionFormat()
	} else {
		backupMetadata.DataFormat = "directory"
	}
	newBackupMetadataBody, err := json.MarshalIndent(backupMetadata, "", "\t")
	if err != nil {
		return err
	}
	remoteBackupMetaFile := path.Join(backupName, "metadata.json")
	if err := b.dst.PutFile(remoteBackupMetaFile,
		ioutil.NopCloser(bytes.NewReader(newBackupMetadataBody))); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	log.
		WithField("duration", utils.HumanizeDuration(time.Since(startUpload))).
		WithField("size", utils.FormatBytes(compressedDataSize+metadataSize+int64(len(newBackupMetadataBody))+backupMetadata.RBACSize+backupMetadata.ConfigSize)).
		Info("done")

	// Clean
	if err := b.dst.RemoveOldBackups(b.cfg.General.BackupsToKeepRemote); err != nil {
		return fmt.Errorf("can't remove old backups on remote storage: %v", err)
	}
	return nil
}

func (b *Backuper) uploadConfigData(backupName string) (int64, error) {
	configBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "configs")
	configFilesGlobPattern := path.Join(configBackupPath, "**/*.*")
	remoteConfigsArchive := path.Join(backupName, fmt.Sprintf("configs.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(configBackupPath, configFilesGlobPattern, remoteConfigsArchive)

}

func (b *Backuper) uploadRBACData(backupName string) (int64, error) {
	rbacBackupPath := path.Join(b.DefaultDataPath, "backup", backupName, "access")
	accessFilesGlobPattern := path.Join(rbacBackupPath, "*.*")
	remoteRBACArchive := path.Join(backupName, fmt.Sprintf("access.%s", b.cfg.GetArchiveExtension()))
	return b.uploadAndArchiveBackupRelatedDir(rbacBackupPath, accessFilesGlobPattern, remoteRBACArchive)
}

func (b *Backuper) uploadAndArchiveBackupRelatedDir(localBackupRelatedDir, localFilesGlobPattern, remoteFile string) (int64, error) {
	if _, err := os.Stat(localBackupRelatedDir); os.IsNotExist(err) {
		return 0, nil
	}
	var localFiles []string
	var err error
	if localFiles, err = filepathx.Glob(localFilesGlobPattern); err != nil || localFiles == nil || len(localFiles) == 0 {
		return 0, fmt.Errorf("list %s return list=%v with err=%v", localFilesGlobPattern, localFiles, err)
	}
	for i := range localFiles {
		localFiles[i] = strings.Replace(localFiles[i], localBackupRelatedDir, "", 1)
	}

	if err := b.dst.CompressedStreamUpload(localBackupRelatedDir, localFiles, remoteFile); err != nil {
		return 0, fmt.Errorf("can't RBAC upload: %v", err)
	}
	remoteUploaded, err := b.dst.StatFile(remoteFile)
	if err != nil {
		return 0, fmt.Errorf("can't check uploaded %s file: %v", remoteFile, err)
	}
	return remoteUploaded.Size(), nil
}

func (b *Backuper) uploadTableData(backupName string, table metadata.TableMetadata) (map[string][]string, int64, error) {
	uuid := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
	metdataFiles := map[string][]string{}
	var uploadedBytes int64
	for disk := range table.Parts {
		backupPath := path.Join(b.DiskMap[disk], "backup", backupName, "shadow", uuid, disk)
		parts, err := separateParts(backupPath, table.Parts[disk], b.cfg.General.MaxFileSize)
		if err != nil {
			return nil, 0, err
		}
		for i, p := range parts {
			remoteDataPath := path.Join(backupName, "shadow", clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Table))
			// Disabled temporary
			// if b.cfg.GetCompressionFormat() == "none" {
			// 	err = b.dst.UploadPath(0, backupPath, p, path.Join(remoteDataPath, disk))
			// } else {
			fileName := fmt.Sprintf("%s_%d.%s", disk, i+1, b.cfg.GetArchiveExtension())
			metdataFiles[disk] = append(metdataFiles[disk], fileName)
			remoteDataFile := path.Join(remoteDataPath, fileName)
			if err := b.dst.CompressedStreamUpload(backupPath, p, remoteDataFile); err != nil {
				return nil, 0, fmt.Errorf("can't upload: %v", err)
			}
			remoteFile, err := b.dst.StatFile(remoteDataFile)
			if err != nil {
				return nil, 0, fmt.Errorf("can't check uploaded file: %v", err)
			}
			uploadedBytes += remoteFile.Size()
		}
	}
	return metdataFiles, uploadedBytes, nil
}

func (b *Backuper) uploadTableMetadata(backupName string, table metadata.TableMetadata) (int64, error) {
	// заливаем метадату для таблицы
	tableMetafile := table
	content, err := json.MarshalIndent(&tableMetafile, "", "\t")
	if err != nil {
		return 0, fmt.Errorf("can't marshal json: %v", err)
	}
	remoteTableMetaFile := path.Join(backupName, "metadata", clickhouse.TablePathEncode(table.Database), fmt.Sprintf("%s.%s", clickhouse.TablePathEncode(table.Table), "json"))
	if err := b.dst.PutFile(remoteTableMetaFile,
		ioutil.NopCloser(bytes.NewReader(content))); err != nil {
		return 0, fmt.Errorf("can't upload: %v", err)
	}
	return int64(len(content)), nil
}

func (b *Backuper) markDuplicatedParts(backup *metadata.BackupMetadata, existsTable *metadata.TableMetadata, newTable *metadata.TableMetadata) {
	for disk, newParts := range newTable.Parts {
		if _, ok := existsTable.Parts[disk]; ok {
			if len(existsTable.Parts[disk]) == 0 {
				continue
			}
			existsPartsMap := map[string]struct{}{}
			for _, p := range existsTable.Parts[disk] {
				existsPartsMap[p.Name] = struct{}{}
			}
			for i := range newParts {
				if _, ok := existsPartsMap[newParts[i].Name]; !ok {
					continue
				}
				uuid := path.Join(clickhouse.TablePathEncode(existsTable.Database), clickhouse.TablePathEncode(existsTable.Table))
				existsPath := path.Join(b.DiskMap[disk], "backup", backup.RequiredBackup, "shadow", uuid, disk, newParts[i].Name)
				newPath := path.Join(b.DiskMap[disk], "backup", backup.BackupName, "shadow", uuid, disk, newParts[i].Name)

				if err := isDuplicatedParts(existsPath, newPath); err != nil {
					apexLog.Debugf("part '%s' and '%s' must be the same: %v", existsPath, newPath, err)
					continue
				}
				newParts[i].Required = true
			}
		}
	}
}

func isDuplicatedParts(part1, part2 string) error {
	p1, err := os.Open(part1)
	if err != nil {
		return err
	}
	defer p1.Close()
	p2, err := os.Open(part2)
	if err != nil {
		return err
	}
	defer p2.Close()
	pf1, err := p1.Readdirnames(-1)
	if err != nil {
		return err
	}
	pf2, err := p2.Readdirnames(-1)
	if err != nil {
		return err
	}
	if len(pf1) != len(pf2) {
		return fmt.Errorf("files count in parts is different")
	}
	for _, f := range pf1 {
		part1File, err := os.Stat(path.Join(part1, f))
		if err != nil {
			return err
		}
		part2File, err := os.Stat(path.Join(part2, f))
		if err != nil {
			return err
		}
		if !os.SameFile(part1File, part2File) {
			return fmt.Errorf("file '%s' is different", f)
		}
	}
	return nil
}

func (b *Backuper) ReadBackupMetadata(backupName string) (*metadata.BackupMetadata, error) {
	backupMetadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
	backupMetadataBody, err := ioutil.ReadFile(backupMetadataPath)
	if err != nil {
		return nil, err
	}
	backupMetadata := metadata.BackupMetadata{}
	if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
		return nil, err
	}
	if len(backupMetadata.Tables) == 0 && !b.cfg.General.AllowEmptyBackups {
		return nil, fmt.Errorf("'%s' is empty backup", backupName)
	}
	return &backupMetadata, nil
}

func separateParts(basePath string, parts []metadata.Part, maxSize int64) ([][]string, error) {
	var size int64
	files := []string{}
	result := [][]string{}
	for i := range parts {
		if parts[i].Required {
			continue
		}
		partPath := path.Join(basePath, parts[i].Name)
		filepath.Walk(partPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if (size + info.Size()) > maxSize {
				result = append(result, files)
				files = []string{}
				size = 0
			}
			relativePath := strings.TrimPrefix(filePath, basePath)
			files = append(files, relativePath)
			size += info.Size()
			return nil
		})
	}
	if len(files) > 0 {
		result = append(result, files)
	}
	return result, nil
}
