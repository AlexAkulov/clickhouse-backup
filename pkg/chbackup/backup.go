package chbackup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// BackupTimeFormat - default backup name format
	BackupTimeFormat = "2006-01-02T15-04-05"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

func addTable(tables []Table, table Table) []Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables BackupTables, table BackupTable) BackupTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addRestoreTable(tables RestoreTables, table RestoreTable) RestoreTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func parseTablePatternForFreeze(tables []Table, tablePattern string) []Table {
	if tablePattern == "" {
		return tables
	}
	tablePatterns := strings.Split(tablePattern, ",")
	var result []Table
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(pattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = addTable(result, t)
			}
		}
	}
	return result
}

func parseTablePatternForRestoreData(tables map[string]BackupTable, tablePattern string) []BackupTable {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	result := BackupTables{}
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
			if matched, _ := filepath.Match(pattern, tableName); matched {
				result = addBackupTable(result, t)
			}
		}
	}
	result.Sort()
	return result
}

func parseSchemaPattern(metadataPath string, tablePattern string) (RestoreTables, error) {
	regularTables := RestoreTables{}
	distributedTables := RestoreTables{}
	viewTables := RestoreTables{}
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(filePath, ".sql") || !info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		p = strings.Trim(strings.TrimPrefix(strings.TrimSuffix(p, ".sql"), metadataPath), "/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(parts[0])
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(p, tableName); matched {
				data, err := ioutil.ReadFile(filePath)
				if err != nil {
					return err
				}
				restoreTable := RestoreTable{
					Database: database,
					Table:    table,
					Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
					Path:     filePath,
				}
				if strings.Contains(restoreTable.Query, "ENGINE = Distributed") {
					distributedTables = addRestoreTable(distributedTables, restoreTable)
					return nil
				}
				if strings.HasPrefix(restoreTable.Query, "CREATE VIEW") ||
					strings.HasPrefix(restoreTable.Query, "CREATE MATERIALIZED VIEW") {
					viewTables = addRestoreTable(viewTables, restoreTable)
					return nil
				}
				regularTables = addRestoreTable(regularTables, restoreTable)
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	regularTables.Sort()
	distributedTables.Sort()
	viewTables.Sort()
	result := append(regularTables, distributedTables...)
	result = append(result, viewTables...)
	return result, nil
}

// PrintTables - print all tables suitable for backup
func PrintTables(config Config) error {
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables with: %v", err)
	}
	for _, table := range allTables {
		if table.Skip {
			fmt.Printf("%s.%s\t(ignored)\n", table.Database, table.Name)
		} else {
			fmt.Printf("%s.%s\n", table.Database, table.Name)
		}
	}
	return nil
}

func restoreSchema(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		PrintLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	metadataPath := path.Join(dataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with %v", err)
	}
	defer ch.Close()

	for _, schema := range tablesForRestore {
		if err := ch.CreateDatabase(schema.Database); err != nil {
			return fmt.Errorf("can't create database `%s` %v", schema.Database, err)
		}
		if err := ch.CreateTable(schema); err != nil {
			return fmt.Errorf("can't create table `%s`.`%s` %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

func printBackups(backupList []Backup, format string, printSize bool) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].Name)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no penult backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].Name)
	case "all", "":
		if len(backupList) == 0 {
			fmt.Println("no backups found")
		}
		for _, backup := range backupList {
			if printSize {
				fmt.Printf("- '%s'\t%s\t(created at %s)\n", backup.Name, FormatBytes(backup.Size), backup.Date.Format("02-01-2006 15:04:05"))
			} else {
				fmt.Printf("- '%s'\t(created at %s)\n", backup.Name, backup.Date.Format("02-01-2006 15:04:05"))
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

// PrintLocalBackups - print all backups stored locally
func PrintLocalBackups(config Config, format string) error {
	backupList, err := ListLocalBackups(config)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackups(backupList, format, false)
}

// ListLocalBackups - return slice of all backups stored locally
func ListLocalBackups(config Config) ([]Backup, error) {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return nil, ErrUnknownClickhouseDataPath
	}
	backupsPath := path.Join(dataPath, "backup")
	d, err := os.Open(backupsPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	result := []Backup{}
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		info, err := os.Stat(path.Join(backupsPath, name))
		if err != nil {
			continue
		}
		if !info.IsDir() {
			continue
		}
		result = append(result, Backup{
			Name: name,
			Date: info.ModTime(),
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

// PrintRemoteBackups - print all backups stored on remote storage
func PrintRemoteBackups(config Config, format string) error {
	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return err
	}

	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	return printBackups(backupList, format, true)
}

// Freeze - freeze tables by tablePattern
func Freeze(config Config, tablePattern, name string) error {
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDataPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse with: %v\nyou can set data_path in config file", err)
	}

	shadowPath := filepath.Join(dataPath, "shadow")
	_, err = ioutil.ReadDir(shadowPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("can't read %s directory: %v", shadowPath, err)
		}
	}
	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get Clickhouse tables with: %v", err)
	}
	backupTables := parseTablePatternForFreeze(allTables, tablePattern)
	if len(backupTables) == 0 {
		return fmt.Errorf("there are no tables in Clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			log.Printf("Skip `%s`.`%s`", table.Database, table.Name)
			continue
		}
		if err := ch.FreezeTable(table, name); err != nil {
			return err
		}
	}
	return nil
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(BackupTimeFormat)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(config Config, backupName, tablePattern string) error {
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	version, err := ch.GetVersion()
	if err != nil {
		ch.Close()
		return err
	}
	ch.Close()
	if backupName == "" {
		backupName = NewBackupName()
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	if _, err := os.Stat(backupPath); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("can't create backup with '%s' already exists", backupPath)
	}
	if err := os.MkdirAll(backupPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create backup with %v", err)
	}
	log.Printf("Create backup '%s'", backupName)
	if err := Freeze(config, tablePattern, backupName); err != nil {
		return err
	}
	log.Println("Copy metadata")
	schemaList, err := parseSchemaPattern(path.Join(dataPath, "metadata"), tablePattern)
	if err != nil {
		return err
	}
	for _, schema := range schemaList {
		skip := false
		for _, filter := range config.ClickHouse.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", schema.Database, schema.Table)); matched {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		relativePath := strings.Trim(strings.TrimPrefix(schema.Path, path.Join(dataPath, "metadata")), "/")
		newPath := path.Join(backupPath, "metadata", relativePath)
		if err := copyFile(schema.Path, newPath); err != nil {
			return fmt.Errorf("can't backup metadata with %v", err)
		}
	}
	log.Println("  Done.")

	log.Println("Move shadow")
	backupShadowDir := path.Join(backupPath, "shadow")
	if err := os.MkdirAll(backupShadowDir, os.ModePerm); err != nil {
		return err
	}
	shadowDir := path.Join(dataPath, "shadow")
	if err := moveShadow(shadowDir, backupShadowDir, backupName, version); err != nil {
		return err
	}
	if err := RemoveOldBackupsLocal(config); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(config Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool) error {
	if schemaOnly || (schemaOnly == dataOnly) {
		err := restoreSchema(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		err := RestoreData(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		fmt.Println("Select backup for restore:")
		PrintLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickouse with: %v", err)
	}
	defer ch.Close()

	allBackupTables, err := ch.GetBackupTables(backupName)
	if err != nil {
		return err
	}
	restoreTables := parseTablePatternForRestoreData(allBackupTables, tablePattern)
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("backup doesn't have tables to restore")
	}
	missingTables := []string{}
	for _, restoreTable := range restoreTables {
		found := false
		for _, chTable := range chTables {
			if (restoreTable.Database == chTable.Database) && (restoreTable.Name == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("'%s.%s'", restoreTable.Database, restoreTable.Name))
		}
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}
	for _, table := range restoreTables {
		if err := ch.CopyData(table); err != nil {
			return fmt.Errorf("can't restore `%s`.`%s` with %v", table.Database, table.Name, err)
		}
		if err := ch.AttachPatritions(table); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s' with %v", table.Database, table.Name, err)
		}
		if err := ch.CheckData(table); err != nil {
			return fmt.Errorf("data in the '%s' is corrupted with %v", table.Database, err)
		}
	}
	return nil
}

func getDataPath(config Config) string {
	if config.ClickHouse.DataPath != "" {
		return config.ClickHouse.DataPath
	}
	ch := &ClickHouse{Config: &config.ClickHouse}
	if err := ch.Connect(); err != nil {
		return ""
	}
	defer ch.Close()
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return ""
	}
	return dataPath
}

func GetLocalBackup(config Config, backupName string) error {
	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

func Upload(config Config, backupName string, diffFrom string) error {
	if backupName == "" {
		fmt.Println("Select backup for upload:")
		PrintLocalBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}

	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to %s with : %v", bd.Kind(), err)
	}

	if err := GetLocalBackup(config, backupName); err != nil {
		return fmt.Errorf("can't upload with %s", err)
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	log.Printf("Upload backup '%s'", backupName)
	diffFromPath := ""
	if diffFrom != "" {
		diffFromPath = path.Join(dataPath, "backup", diffFrom)
	}
	if err := bd.CompressedStreamUpload(backupPath, backupName, diffFromPath); err != nil {
		return fmt.Errorf("can't upload with %v", err)
	}
	if err := bd.RemoveOldBackups(bd.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Println("  Done.")
	return nil
}

func Download(config Config, backupName string) error {
	if backupName == "" {
		fmt.Println("Select backup for download:")
		PrintRemoteBackups(config, "all")
		os.Exit(1)
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return err
	}
	err = bd.CompressedStreamDownload(backupName, path.Join(dataPath, "backup", backupName))
	if err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Clean - removed all data in shadow folder
func Clean(config Config) error {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	shadowDir := path.Join(dataPath, "shadow")
	if _, err := os.Stat(shadowDir); os.IsNotExist(err) {
		log.Printf("%s directory does not exist, nothing to do", shadowDir)
		return nil
	}
	log.Printf("Clean %s", shadowDir)
	if err := cleanDir(shadowDir); err != nil {
		return fmt.Errorf("can't remove contents from directory %v: %v", shadowDir, err)
	}
	return nil
}

//
func RemoveOldBackupsLocal(config Config) error {
	if config.General.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupsToDelete := GetBackupsToDelete(backupList, config.General.BackupsToKeepLocal)
	for _, backup := range backupsToDelete {
		backupPath := path.Join(dataPath, "backup", backup.Name)
		os.RemoveAll(backupPath)
	}
	return nil
}

func RemoveBackupLocal(config Config, backupName string) error {
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return os.RemoveAll(path.Join(dataPath, "backup", backupName))
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

func RemoveBackupRemote(config Config, backupName string) error {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}

	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to remote storage with: %v", err)
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return bd.RemoveBackup(backupName)
		}
	}
	return fmt.Errorf("backup '%s' not found on remote storage", backupName)
}
