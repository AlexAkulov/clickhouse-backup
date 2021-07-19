package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/utils"
	apexLog "github.com/apex/log"
	"github.com/google/uuid"
	"github.com/otiai10/copy"
)

const (
	// TimeFormatForBackup - default backup name format
	TimeFormatForBackup = "2006-01-02T15-04-05"
	hashfile            = "parts.hash"
	MetaFileName        = "metadata.json"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

type BackupLocal struct {
	metadata.BackupMetadata
	Legacy bool
	Broken string
}

func addTable(tables []clickhouse.Table, table clickhouse.Table) []clickhouse.Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables clickhouse.BackupTables, table metadata.TableMetadata) clickhouse.BackupTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func filterTablesByPattern(tables []clickhouse.Table, tablePattern string) []clickhouse.Table {
	if tablePattern == "" {
		return tables
	}
	tablePatterns := strings.Split(tablePattern, ",")
	var result []clickhouse.Table
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(pattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = addTable(result, t)
			}
		}
	}
	return result
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(TimeFormatForBackup)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(cfg *config.Config, backupName, tablePattern string, schemaOnly, doBackupRBAC, doBackupConfig bool, version string) error {
	startBackup := time.Now()
	if backupName == "" {
		backupName = NewBackupName()
	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
	})
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allDatabases, err := ch.GetDatabases()
	if err != nil {
		return fmt.Errorf("cat't get database engines from clickhouse: %v", err)
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("cat't get tables from clickhouse: %v", err)
	}
	tables := filterTablesByPattern(allTables, tablePattern)
	i := 0
	for _, table := range tables {
		if table.Skip {
			continue
		}
		i++
	}
	if i == 0 && !cfg.General.AllowEmptyBackups {
		return fmt.Errorf("no tables for backup")
	}

	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	// create backup dir on all clickhouse disks
	for _, disk := range disks {
		if err := ch.Mkdir(path.Join(disk.Path, "backup")); err != nil {
			return err
		}
	}
	defaultPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	backupPath := path.Join(defaultPath, "backup", backupName)
	if _, err := os.Stat(path.Join(backupPath, "metadata.json")); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("'%s' medatata.json already exists", backupName)
	}
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err = ch.Mkdir(backupPath); err != nil {
			log.Errorf("can't create diretory %s: %v", backupPath, err)
		}
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	var backupDataSize, backupMetadataSize int64

	var t []metadata.TableTitle
	for _, table := range tables {
		log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name))
		if table.Skip {
			continue
		}
		var realSize map[string]int64
		var partitions map[string][]metadata.Part
		if !schemaOnly {
			log.Debug("create data")
			partitions, realSize, err = AddTableToBackup(ch, backupName, &table)
			if err != nil {
				log.Error(err.Error())
				if removeBackupErr := RemoveBackupLocal(cfg, backupName); removeBackupErr != nil {
					log.Error(removeBackupErr.Error())
				}
				// continue
				return err
			}
			backupDataSize += table.TotalBytes.Int64
		}
		log.Debug("create metadata")
		metadataSize, err := createMetadata(ch, backupPath, metadata.TableMetadata{
			Table:      table.Name,
			Database:   table.Database,
			Query:      table.CreateTableQuery,
			TotalBytes: table.TotalBytes.Int64,
			Size:       realSize,
			Parts:      partitions,
		})
		if err != nil {
			if removeBackupErr := RemoveBackupLocal(cfg, backupName); removeBackupErr != nil {
				log.Error(removeBackupErr.Error())
			}
			return err
			// continue
		}
		backupMetadataSize += int64(metadataSize)
		t = append(t, metadata.TableTitle{
			Database: table.Database,
			Table:    table.Name,
		})
		log.Infof("done")
	}
	backupRBACSize, backupConfigSize := int64(0), int64(0)

	if doBackupRBAC {
		if backupRBACSize, err = createRBACBackup(ch, backupPath, disks); err != nil {
			log.Errorf("error during do RBAC backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupRBACSize)).Info("done createRBACBackup")
		}
	}
	if doBackupConfig {
		if backupConfigSize, err = createConfigBackup(cfg, backupPath); err != nil {
			log.Errorf("error during do CONFIG backup: %v", err)
		} else {
			log.WithField("size", utils.FormatBytes(backupConfigSize)).Info("done createConfigBackup")
		}
	}

	backupMetadata := metadata.BackupMetadata{
		// TODO: надо помечать какие таблички зафейлились либо фейлить весь бэкап
		BackupName:              backupName,
		Disks:                   diskMap,
		ClickhouseBackupVersion: version,
		CreationDate:            time.Now().UTC(),
		// Tags: ,
		ClickHouseVersion: ch.GetVersionDescribe(),
		DataSize:          backupDataSize,
		MetadataSize:      backupMetadataSize,
		RBACSize:          backupRBACSize,
		ConfigSize:        backupConfigSize,
		// CompressedSize: ,
		Tables:    t,
		Databases: []metadata.DatabasesMeta{},
	}
	for _, database := range allDatabases {
		backupMetadata.Databases = append(backupMetadata.Databases, metadata.DatabasesMeta(database))
	}
	content, err := json.MarshalIndent(&backupMetadata, "", "\t")
	if err != nil {
		_ = RemoveBackupLocal(cfg, backupName)
		return fmt.Errorf("can't marshal backup metafile json: %v", err)
	}
	backupMetaFile := path.Join(defaultPath, "backup", backupName, "metadata.json")
	if err := ioutil.WriteFile(backupMetaFile, content, 0640); err != nil {
		_ = RemoveBackupLocal(cfg, backupName)
		return err
	}
	if err := ch.Chown(backupMetaFile); err != nil {
		log.Warnf("can't chown %s: %v", backupMetaFile, err)
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startBackup))).Info("done")

	// Clean
	if err := RemoveOldBackupsLocal(cfg, true); err != nil {
		return err
	}
	return nil
}

func createConfigBackup(cfg *config.Config, backupPath string) (int64, error) {
	backupConfigSize := int64(0)
	configBackupPath := path.Join(backupPath, "configs")
	apexLog.Debugf("copy %s -> %s", cfg.ClickHouse.ConfigDir, configBackupPath)
	copyErr := copy.Copy(cfg.ClickHouse.ConfigDir, configBackupPath, copy.Options{
		Skip: func(src string) (bool, error) {
			if fileInfo, err := os.Stat(src); err == nil {
				backupConfigSize += fileInfo.Size()
			}
			return false, nil
		},
	})
	return backupConfigSize, copyErr
}

func createRBACBackup(ch *clickhouse.ClickHouse, backupPath string, disks []clickhouse.Disk) (int64, error) {
	rbacDataSize := int64(0)
	rbacBackup := path.Join(backupPath, "access")
	accessPath, err := ch.GetAccessManagementPath(disks)
	if err != nil {
		return 0, err
	}
	apexLog.Debugf("copy %s -> %s", accessPath, rbacBackup)
	copyErr := copy.Copy(accessPath, rbacBackup, copy.Options{
		Skip: func(src string) (bool, error) {
			if fileInfo, err := os.Stat(src); err == nil {
				rbacDataSize += fileInfo.Size()
			}
			return false, nil
		},
	})
	return rbacDataSize, copyErr
}

func AddTableToBackup(ch *clickhouse.ClickHouse, backupName string, table *clickhouse.Table) (map[string][]metadata.Part, map[string]int64, error) {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "create",
		"table":     fmt.Sprintf("%s.%s", table.Database, table.Name),
	})
	if backupName == "" {
		return nil, nil, fmt.Errorf("backupName is not defined")
	}
	diskList, err := ch.GetDisks()
	if err != nil {
		return nil, nil, fmt.Errorf("can't get clickhouse disk list: %v", err)
	}

	// backup data
	if !strings.HasSuffix(table.Engine, "MergeTree") {
		log.WithField("engine", table.Engine).Debug("skipped")
		return nil, nil, nil
	}
	backupID := strings.ReplaceAll(uuid.New().String(), "-", "")
	if err := ch.FreezeTable(table, backupID); err != nil {
		return nil, nil, err
	}
	log.Debug("freezed")
	realSize := map[string]int64{}
	partitions := map[string][]metadata.Part{}
	for _, disk := range diskList {
		shadowPath := path.Join(disk.Path, "shadow", backupID)
		if _, err := os.Stat(shadowPath); err != nil && os.IsNotExist(err) {
			continue
		}
		backupPath := path.Join(disk.Path, "backup", backupName)
		encodedTablePath := path.Join(clickhouse.TablePathEncode(table.Database), clickhouse.TablePathEncode(table.Name))
		backupShadowPath := path.Join(backupPath, "shadow", encodedTablePath, disk.Name)
		if err := ch.MkdirAll(backupShadowPath); err != nil && !os.IsExist(err) {
			return nil, nil, err
		}
		parts, size, err := moveShadow(shadowPath, backupShadowPath)
		if err != nil {
			return nil, nil, err
		}
		realSize[disk.Name] = size
		partitions[disk.Name] = parts
		log.WithField("disk", disk.Name).Debug("shadow moved")
		if err := os.RemoveAll(shadowPath); err != nil {
			return partitions, realSize, err
		}
	}
	if err := ch.CleanShadow(backupID); err != nil {
		return partitions, realSize, err
	}
	log.Debug("done")
	return partitions, realSize, nil
}

func createMetadata(ch *clickhouse.ClickHouse, backupPath string, table metadata.TableMetadata) (int, error) {
	metadataPath := path.Join(backupPath, "metadata")
	if err := ch.Mkdir(metadataPath); err != nil {
		return 0, err
	}
	metadataDatabasePath := path.Join(metadataPath, clickhouse.TablePathEncode(table.Database))
	if err := ch.Mkdir(metadataDatabasePath); err != nil {
		return 0, err
	}
	metadataFile := path.Join(metadataDatabasePath, fmt.Sprintf("%s.json", clickhouse.TablePathEncode(table.Table)))
	metadataBody, err := json.MarshalIndent(&table, "", " ")
	if err != nil {
		return 0, fmt.Errorf("can't marshal %s: %v", MetaFileName, err)
	}
	if err := ioutil.WriteFile(metadataFile, metadataBody, 0644); err != nil {
		return 0, fmt.Errorf("can't create %s: %v", MetaFileName, err)
	}
	if err := ch.Chown(metadataFile); err != nil {
		return 0, err
	}
	return len(metadataBody), nil
}
