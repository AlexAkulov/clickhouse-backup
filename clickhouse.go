package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

// ClickHouse - provide info and freeze tables
type ClickHouse struct {
	DryRun bool
	Config *ClickHouseConfig
	conn   *sqlx.DB
	uid    *int
	gid    *int
}

// Table - Clickhouse table struct
type Table struct {
	Database     string `db:"database"`
	Name         string `db:"name"`
	DataPath     string `db:"data_path"`
	MetadataPath string `db:"metadata_path"`
	IsTemporary  bool   `db:"is_temporary"`
}

// BackupPartition - struct representing Clickhouse partition
type BackupPartition struct {
	Name string
	Path string
}

// BackupTable - struct to store additional information on partitions
type BackupTable struct {
	Increment  int
	Database   string
	Name       string
	Partitions []BackupPartition
	Path       string
}

// RestoreTable - struct to store information needed during restore
type RestoreTable struct {
	Database string
	Query    string
}

// Connect - connect to clickhouse
func (ch *ClickHouse) Connect() error {
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&compress=true",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// ConnectDatabase - connect to clickhouse to specified database
func (ch *ClickHouse) ConnectDatabase(database string) error {
	if database == "" {
		database = "default"
	}
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&database=%v&compress=true",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password, database)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// GetDataPath - return clickhouse data_path
func (ch *ClickHouse) GetDataPath() (string, error) {
	if ch.Config.DataPath != "" {
		return ch.Config.DataPath, nil
	}
	var result []struct {
		MetadataPath string `db:"metadata_path"`
	}
	if err := ch.conn.Select(&result, "SELECT metadata_path FROM system.tables WHERE database == 'system' LIMIT 1;"); err != nil {
		return "/var/lib/clickhouse", err
	}
	metadataPath := result[0].MetadataPath
	dataPathArray := strings.Split(metadataPath, "/")
	clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-3]...)
	return path.Join("/", clickhouseData), nil
}

// Close - close connection to clickhouse
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

// GetTables - get all tables info
func (ch *ClickHouse) GetTables() ([]Table, error) {
	var tables []Table
	if err := ch.conn.Select(&tables, "SELECT database, name, is_temporary, data_path, metadata_path FROM system.tables WHERE database != 'system';"); err != nil {
		return nil, err
	}
	return tables, nil
}

// FreezeTable - freeze all partitions for table
func (ch *ClickHouse) FreezeTable(table Table) error {
	var partitions []struct {
		PartitionID string `db:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM system.parts WHERE database='%v' AND table='%v'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for \"%s.%s\" with %v", table.Database, table.Name, err)
	}
	log.Printf("Freeze '%v.%v'", table.Database, table.Name)
	for _, item := range partitions {
		if ch.DryRun {
			log.Printf("  partition '%v'   ...skip because dry-run", item.PartitionID)
			continue
		}
		log.Printf("  partition '%v'", item.PartitionID)
		// TODO: find out this magic
		if item.PartitionID == "all" {
			item.PartitionID = "tuple()"
		}
		if _, err := ch.conn.Exec(
			fmt.Sprintf(
				"ALTER TABLE %v.%v FREEZE PARTITION %v;",
				table.Database,
				table.Name,
				item.PartitionID,
			)); err == nil {
			continue
		}
		if _, err := ch.conn.Exec(
			fmt.Sprintf(
				"ALTER TABLE %v.%v FREEZE PARTITION '%v';",
				table.Database,
				table.Name,
				item.PartitionID,
			)); err != nil {
			return fmt.Errorf("can't freeze partition '%s' on '%s.%s' with: %v", item.PartitionID, table.Database, table.Name, err)
		}
	}
	return nil
}

// GetBackupTables - return list of backups of tables that can be restored
func (ch *ClickHouse) GetBackupTables() (map[string]BackupTable, error) {
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return nil, err
	}
	backupShadowPath := filepath.Join(dataPath, "backup", "shadow")
	result := make(map[string]BackupTable)
	if err := filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
			parts := strings.Split(relativePath, "/")
			if len(parts) != 5 {
				return nil
			}
			partition := BackupPartition{
				Name: parts[4],
				Path: filePath,
			}
			increment, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil
			}
			table := BackupTable{
				Increment:  increment,
				Database:   parts[2],
				Name:       parts[3],
				Partitions: []BackupPartition{partition},
			}
			fullTableName := fmt.Sprintf("%s.%s-%d", table.Database, table.Name, table.Increment)
			if t, ok := result[fullTableName]; ok {
				t.Partitions = append(t.Partitions, partition)
				result[fullTableName] = t
				return nil
			}
			result[fullTableName] = table
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// Chown - set owner and group to clickhouse user
func (ch *ClickHouse) Chown(name string) error {
	var (
		dataPath string
		err      error
	)
	if ch.uid == nil || ch.gid == nil {
		if dataPath, err = ch.GetDataPath(); err != nil {
			return err
		}
		info, err := os.Stat(path.Join(dataPath, "data"))
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		uid := int(stat.Uid)
		gid := int(stat.Gid)
		ch.uid = &uid
		ch.gid = &gid
	}
	return os.Chown(name, *ch.uid, *ch.gid)
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyData(table BackupTable) error {
	if ch.DryRun {
		log.Printf("copy %s.%s increment %d  ...skip dry-run", table.Database, table.Name, table.Increment)
		return nil
	}
	log.Printf("copy %s.%s increment %d", table.Database, table.Name, table.Increment)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}

	detachedParentDir := filepath.Join(dataPath, "data", table.Database, table.Name, "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range table.Partitions {
		detachedPath := filepath.Join(detachedParentDir, partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("%v", err)
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' should not exist", detachedPath)
		}
		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix Windows slashes
			srcFileStat, err := os.Stat(filePath)
			if err != nil {
				return err
			}
			filename := strings.Trim(strings.TrimPrefix(filePath, partition.Path), "/")
			dstFilePath := filepath.Join(detachedPath, filename)
			if srcFileStat.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !srcFileStat.Mode().IsRegular() {
				return fmt.Errorf("'%s' is not a regular file", filePath)
			}
			srcFile, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer srcFile.Close()
			dstFile, err := os.Create(dstFilePath)
			if err != nil {
				return err
			}
			defer dstFile.Close()
			if _, err = io.Copy(dstFile, srcFile); err != nil {
				return err
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return err
		}
	}
	return nil
}

func convertPartition(detachedTableFolder string, deprecatedCreation bool) (string, error) {
	// TODO: need tests
	begin := strings.Split(detachedTableFolder, "_")[0]
	if begin == "all" {
		// table is not partitioned at all
		// ENGINE = MergeTree ORDER BY id
		return "tuple()", nil
	}
	if deprecatedCreation {
		// legacy partitioning based on month: toYYYYMM(date_column)
		// in this case we return YYYYMM
		// ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)
		if len(begin) < 6 {
			return "", fmt.Errorf("deprecated type of partitioning was requested, but partition name of table does not correspond that")
		}
		return begin[:6], nil
	}
	// in case a custom partitioning key is used this is a partition name
	// same as in system.parts table, it may be used in ALTER TABLE queries
	// https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/
	return begin, nil
}

// AttachPatritions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPatritions(table BackupTable, deprecatedCreation bool) error {
	// TODO: need tests
	partitionName, err := convertPartition(table.Partitions[0].Name, deprecatedCreation)
	if err != nil {
		return err
	}
	if ch.DryRun {
		log.Printf("ATTACH partition '%s' for %s.%s increment %d ...skip dry-run", partitionName, table.Database, table.Name, table.Increment)
		return nil
	}
	log.Printf("ATTACH partitions for %s.%s increment %d", table.Database, table.Name, table.Increment)
	if _, err := ch.conn.Exec(fmt.Sprintf("ALTER TABLE %v.%v ATTACH PARTITION %s", table.Database, table.Name, partitionName)); err != nil {
		return err
	}
	return nil
}

// CreateDatabase - create specific database from metadata in backup folder
func (ch *ClickHouse) CreateDatabase(database string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
	if ch.DryRun {
		log.Printf("DRY-RUN: creating database with query: %s", createQuery)
		return nil
	}
	log.Printf("creating database:\n%s", database)
	_, err := ch.conn.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("can't create database: %v", err)
	}
	return nil
}

// CreateTable - create specific table from metadata in backup folder
func (ch *ClickHouse) CreateTable(table RestoreTable) error {
	if ch.DryRun {
		log.Printf("DRY-RUN: creating table with query: %s", table.Query)
		return nil
	}
	ch.ConnectDatabase(table.Database)
	log.Printf("creating table:\n%s", table.Query)
	_, err := ch.conn.Exec(table.Query)
	if err != nil {
		return fmt.Errorf("can't create table: %v", err)
	}
	return nil
}
