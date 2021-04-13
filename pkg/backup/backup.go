package backup

import (
	"encoding/json"
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

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	"github.com/AlexAkulov/clickhouse-backup/utils"
)

const (
	// BackupTimeFormat - default backup name format
	BackupTimeFormat                     = "2006-01-02T15-04-05"
	hashfile                             = "parts.hash"
	flashbackAvailableSinceChVersion int = 19150306
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

// RestoreTable - struct to store information needed during restore
type RestoreTable struct {
	Database string
	Table    string
	Query    string
	Path     string
}

// RestoreTables - slice of RestoreTable
type RestoreTables []RestoreTable

// Sort - sorting BackupTables slice orderly by name
func (rt RestoreTables) Sort() {
	sort.Slice(rt, func(i, j int) bool {
		return (rt[i].Database < rt[j].Database) || (rt[i].Database == rt[j].Database && rt[i].Table < rt[j].Table)
	})
}
func addTable(tables []clickhouse.Table, table clickhouse.Table) []clickhouse.Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables clickhouse.BackupTables, table clickhouse.BackupTable) clickhouse.BackupTables {
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

func parseTablePatternForFreeze(tables []clickhouse.Table, tablePattern string) []clickhouse.Table {
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

func parseTablePatternForRestoreData(tables map[string]clickhouse.BackupTable, tablePattern string) []clickhouse.BackupTable {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	result := clickhouse.BackupTables{}
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

	//log.Printf("tp1 = %s", tablePattern)
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(filePath, ".sql") || !info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		//log.Printf("p1 = %s", p)
		p = strings.Trim(strings.TrimPrefix(strings.TrimSuffix(p, ".sql"), metadataPath), "/")
		//log.Printf("p2 = %s", p)
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(parts[0])
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			//log.Printf("p3 = %s", p)
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
			/*else {
				log.Printf("No match %s %s", p, tableName)
			}*/
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

// GetTables - get all tables for use by PrintTables and API
func GetTables(cfg config.Config) ([]clickhouse.Table, error) {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return []clickhouse.Table{}, fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return []clickhouse.Table{}, fmt.Errorf("can't get tables: %v", err)
	}
	return allTables, nil
}

// PrintTables - print all tables suitable for backup
func PrintTables(cfg config.Config) error {
	allTables, err := GetTables(cfg)
	if err != nil {
		return err
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

func RestoreSchema(cfg config.Config, backupName string, tablePattern string, dropTable bool) error {
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}
	dataPath := getDataPath(cfg)
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
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	for _, schema := range tablesForRestore {
		if err := ch.CreateDatabase(schema.Database); err != nil {
			return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
		}
		if err := ch.CreateTable(clickhouse.Table{
			Database: schema.Database,
			Name:     schema.Table,
		}, schema.Query, dropTable); err != nil {
			return fmt.Errorf("can't create table '%s.%s': %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

func printBackups(backupList []storage.Backup, format string, printSize bool) error {
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
				fmt.Printf("- '%s'\t%s\t(created at %s)\n", backup.Name, utils.FormatBytes(backup.Size), backup.Date.Format("02-01-2006 15:04:05"))
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
func PrintLocalBackups(cfg config.Config, format string) error {
	backupList, err := ListLocalBackups(cfg)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackups(backupList, format, false)
}

// ListLocalBackups - return slice of all backups stored locally
func ListLocalBackups(cfg config.Config) ([]storage.Backup, error) {
	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return nil, ErrUnknownClickhouseDataPath
	}
	backupsPath := path.Join(dataPath, "backup")
	d, err := os.Open(backupsPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	result := []storage.Backup{}
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
		result = append(result, storage.Backup{
			Name: name,
			Date: info.ModTime(),
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

// GetRemoteBackups - get all backups stored on remote storage
func GetRemoteBackups(cfg config.Config) ([]storage.Backup, error) {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("PrintRemoteBackups aborted: RemoteStorage set to \"none\"")
		return []storage.Backup{}, nil
	}
	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return []storage.Backup{}, err
	}
	err = bd.Connect()
	if err != nil {
		return []storage.Backup{}, err
	}

	backupList, err := bd.BackupList()
	if err != nil {
		return []storage.Backup{}, err
	}
	return backupList, err
}

// PrintRemoteBackups - print all backups stored on remote storage
func PrintRemoteBackups(cfg config.Config, format string) error {
	backupList, err := GetRemoteBackups(cfg)
	if err != nil {
		return err
	}
	return printBackups(backupList, format, true)
}

// Freeze - freeze tables by tablePattern
func Freeze(cfg config.Config, tablePattern string) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDataPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse: %v\nyou can set data_path in config file", err)
	}

	shadowPath := filepath.Join(dataPath, "shadow")
	files, err := ioutil.ReadDir(shadowPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("can't read %s directory: %v", shadowPath, err)
		}
	}
	if len(files) > 0 {
		if !cfg.ClickHouse.AutoCleanShadow {
			return fmt.Errorf("'%s' is not empty, execute 'clean' command first", shadowPath)
		}
		if err := Clean(cfg); err != nil {
			return err
		}
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
	backupTables := parseTablePatternForFreeze(allTables, tablePattern)
	if len(backupTables) == 0 {
		return fmt.Errorf("there are no tables in clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			log.Printf("Skip '%s.%s'", table.Database, table.Name)
			continue
		}
		if err := ch.FreezeTable(table); err != nil {
			return err
		}
	}
	return nil
}

// CopyPartHashes - Copy data parts hashes by tablePattern
func CopyPartHashes(cfg config.Config, tablePattern string, backupName string) error {
	var allparts map[string][]clickhouse.Partition
	allparts = make(map[string][]clickhouse.Partition)

	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDataPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse: %v\nyou can set data_path in config file", err)
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
	backupTables := parseTablePatternForFreeze(allTables, tablePattern)
	if len(backupTables) == 0 {
		return fmt.Errorf("there are no tables in clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			continue
		}

		parts, err := ch.GetPartitions(table)
		if err != nil {
			return err
		}
		allparts[table.Database+"."+table.Name] = parts
	}
	byteArray, err := json.MarshalIndent(allparts, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	hashPartsPath := path.Join(dataPath, "backup", backupName, hashfile)
	_ = ioutil.WriteFile(hashPartsPath, byteArray, 0644)

	return nil
}

// NewBackupName - return default backup name
func NewBackupName() string {
	return time.Now().UTC().Format(BackupTimeFormat)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(cfg config.Config, backupName, tablePattern string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	if _, err := os.Stat(backupPath); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("can't create backup '%s' already exists", backupPath)
	}
	schemaList, err := parseSchemaPattern(path.Join(dataPath, "metadata"), tablePattern)
	if err != nil {
		return err
	}
	schemaForBackup := []RestoreTable{}
	for _, schema := range schemaList {
		skip := false
		for _, filter := range cfg.ClickHouse.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", schema.Database, schema.Table)); matched {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		schemaForBackup = append(schemaForBackup, schema)
	}
	if len(schemaForBackup) == 0 {
		return fmt.Errorf("no tables for backup, create something first or check 'skip_tables'")
	}
	log.Printf("Create backup '%s'", backupName)
	if err := os.MkdirAll(backupPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create backup: %v", err)
	}
	log.Println("Copy metadata")
	for _, schema := range schemaForBackup {
		relativePath := strings.Trim(strings.TrimPrefix(schema.Path, path.Join(dataPath, "metadata")), "/")
		newPath := path.Join(backupPath, "metadata", relativePath)
		if err := copyFile(schema.Path, newPath); err != nil {
			return fmt.Errorf("can't backup metadata: %v", err)
		}
	}
	log.Println("  Done.")

	if err := Freeze(cfg, tablePattern); err != nil {
		return err
	}
	// log.Printf("Copy part hashes")
	if err := CopyPartHashes(cfg, tablePattern, backupName); err != nil {
		log.Println(err)
	}
	log.Println("Move shadow")
	backupShadowDir := path.Join(backupPath, "shadow")
	if err := os.MkdirAll(backupShadowDir, os.ModePerm); err != nil {
		return err
	}
	shadowDir := path.Join(dataPath, "shadow")
	if err := moveShadow(shadowDir, backupShadowDir); err != nil {
		return err
	}
	if err := RemoveOldBackupsLocal(cfg); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(cfg config.Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool, dropTable bool) error {
	if schemaOnly || (schemaOnly == dataOnly) {
		if err := RestoreSchema(cfg, backupName, tablePattern, dropTable); err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		if err := RestoreData(cfg, backupName, tablePattern); err != nil {
			return err
		}
	}
	return nil
}

// Flashback - restore tables matched by tablePattern from backupName by restroing only modified parts.
func Flashback(cfg config.Config, backupName string, tablePattern string) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if errc := ch.Connect(); errc != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", errc)
	}

	version, errgv := ch.GetVersion()
	if errgv != nil {
		return errgv
	}

	log.Printf("Database Version  = %v", version)
	log.Printf("flashbackAvailableSinceChVersion = %v", flashbackAvailableSinceChVersion)

	if version < flashbackAvailableSinceChVersion {
		errv := fmt.Errorf("Flashback feature is only avalaible for clickhouse version >= %v", flashbackAvailableSinceChVersion)
		return errv
	}

	err := FlashBackData(cfg, backupName, tablePattern)
	if err != nil {
		return err
	}
	return nil
}

// FlashBackData - restore data for tables matched by tablePattern from backupName
func FlashBackData(cfg config.Config, backupName string, tablePattern string) error {
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}

	dataPath := getDataPath(cfg)

	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allBackupTables, err := ch.GetBackupTables(backupName)
	if err != nil {
		return err
	}

	restoreTables := parseTablePatternForRestoreData(allBackupTables, tablePattern)

	liveTables, err := ch.GetTables()

	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("backup doesn't have tables to restore")
	}

	missingTables := []string{}

	for _, restoreTable := range restoreTables {
		found := false
		for _, liveTable := range liveTables {
			if (restoreTable.Database == liveTable.Database) && (restoreTable.Name == liveTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("%s.%s", restoreTable.Database, restoreTable.Name))

			for _, newtable := range missingTables {
				//log.Printf("newtable=%s", newtable)
				if err := RestoreSchema(cfg, backupName, newtable, true); err != nil {
					return err
				}
			}

			FlashBackData(cfg, backupName, tablePattern)
			return nil
		}
	}

	diffInfos, _ := ch.ComputePartitionsDelta(restoreTables, liveTables)
	for _, tableDiff := range diffInfos {
		if err := ch.CopyDataDiff(tableDiff); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", tableDiff.BTable.Database, tableDiff.BTable.Name, err)
		}
		if err := ch.ApplyPartitionsChanges(tableDiff); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", tableDiff.BTable.Database, tableDiff.BTable.Name, err)
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(cfg config.Config, backupName string, tablePattern string) error {
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}
	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
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
			return fmt.Errorf("can't restore '%s.%s': %v", table.Database, table.Name, err)
		}
		if err := ch.AttachPartitions(table); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", table.Database, table.Name, err)
		}
	}
	return nil
}

func getDataPath(cfg config.Config) string {
	if cfg.ClickHouse.DataPath != "" {
		return cfg.ClickHouse.DataPath
	}
	ch := &clickhouse.ClickHouse{Config: &cfg.ClickHouse}
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

func GetLocalBackup(cfg config.Config, backupName string) error {
	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	backupList, err := ListLocalBackups(cfg)
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

func Upload(cfg config.Config, backupName string, diffFrom string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for upload")
	}
	if backupName == diffFrom {
		return fmt.Errorf("You can't upload diff from the same backup")
	}

	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}

	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to %s: %v", bd.Kind(), err)
	}

	if err := GetLocalBackup(cfg, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	log.Printf("Upload backup '%s'", backupName)
	diffFromPath := ""
	if diffFrom != "" {
		diffFromPath = path.Join(dataPath, "backup", diffFrom)
	}
	if clickhouse.IsClickhouseShadow(filepath.Join(diffFromPath, "shadow")) {
		return fmt.Errorf("'%s' is old format backup and doesn't supports diff", filepath.Base(diffFromPath))
	}
	if err := bd.CompressedStreamUpload(backupPath, backupName, diffFromPath); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	if err := bd.RemoveOldBackups(bd.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Println("  Done.")
	return nil
}

func Download(cfg config.Config, backupName string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Download aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintRemoteBackups(cfg, "all")
		return fmt.Errorf("select backup for download")
	}
	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}

	if err := bd.Connect(); err != nil {
		return err
	}
	if err := bd.CompressedStreamDownload(backupName,
		path.Join(dataPath, "backup", backupName)); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Clean - removed all data in shadow folder
func Clean(cfg config.Config) error {
	dataPath := getDataPath(cfg)
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
		return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
	}
	return nil
}

//
func RemoveOldBackupsLocal(cfg config.Config) error {
	if cfg.General.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := ListLocalBackups(cfg)
	if err != nil {
		return err
	}
	dataPath := getDataPath(cfg)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupsToDelete := storage.GetBackupsToDelete(backupList, cfg.General.BackupsToKeepLocal)
	for _, backup := range backupsToDelete {
		backupPath := path.Join(dataPath, "backup", backup.Name)
		os.RemoveAll(backupPath)
	}
	return nil
}

func RemoveBackupLocal(cfg config.Config, backupName string) error {
	backupList, err := ListLocalBackups(cfg)
	if err != nil {
		return err
	}
	dataPath := getDataPath(cfg)
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

func RemoveBackupRemote(cfg config.Config, backupName string) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("RemoveBackupRemote aborted: RemoteStorage set to \"none\"")
		return nil
	}

	bd, err := storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
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

func CreateToRemote(cfg config.Config, backupName, tablePattern, diffFrom string, removeBackup bool) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := CreateBackup(cfg, backupName, tablePattern); err != nil {
		return err
	}
	if err := Upload(cfg, backupName, diffFrom); err != nil {
		return err
	}
	if removeBackup {
		return RemoveBackupLocal(cfg, backupName)
	}
	return nil
}

func RestoreFromRemote(cfg config.Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool, dropTable bool) error {
	if err := Download(cfg, backupName); err != nil {
		return err
	}
	return Restore(cfg, backupName, tablePattern, schemaOnly, dataOnly, dropTable)
}
