package chbackup

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/btcsuite/btcutil/base58"
	"github.com/jmoiron/sqlx"
)

// ClickHouse - provide
type ClickHouse struct {
	Config *ClickHouseConfig
	conn   *sqlx.DB
	uid    *int
	gid    *int
}

// Table - ClickHouse table struct
type Table struct {
	Database string `db:"database"`
	Name     string `db:"name"`
	Skip     bool
}

// BackupPartition - struct representing Clickhouse partition
type BackupPartition struct {
	Name string
	Path string
}

// BackupTable - struct to store additional information on partitions
type BackupTable struct {
	Database   string
	Name       string
	Partitions []BackupPartition
}

// BackupTables - slice of BackupTable
type BackupTables []BackupTable

// Sort - sorting BackupTables slice orderly by name
func (bt BackupTables) Sort() {
	sort.Slice(bt, func(i, j int) bool {
		return (bt[i].Database < bt[j].Database) || (bt[i].Database == bt[j].Database && bt[i].Name < bt[j].Name)
	})
}

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

// Connect - establish connection to ClickHouse
func (ch *ClickHouse) Connect() error {
	timeout, err := time.ParseDuration(ch.Config.Timeout)
	if err != nil {
		return err
	}
	timeoutSeconds := int(timeout.Seconds())
	connectionString := fmt.Sprintf("tcp://%v:%v?username=%v&password=%v&database=system&compress=true&receive_timeout=%d&send_timeout=%d",
		ch.Config.Host, ch.Config.Port, ch.Config.Username, ch.Config.Password, timeoutSeconds, timeoutSeconds)
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

// GetDataPath - return ClickHouse data_path
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

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

// GetTables - return slice of all tables suitable for backup
func (ch *ClickHouse) GetTables() ([]Table, error) {
	var tables []Table
	if err := ch.conn.Select(&tables, "SELECT database, name FROM system.tables WHERE is_temporary = 0 AND engine LIKE '%MergeTree';"); err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				tables[i] = t
				break
			}
		}
	}
	return tables, nil
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion() (int, error) {
	var result []string
	q := fmt.Sprintf("SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'")
	if err := ch.conn.Select(&result, q); err != nil {
		return 0, fmt.Errorf("can't get ClickHouse version with %v", err)
	}
	if len(result) == 0 {
		return 0, nil
	}
	return strconv.Atoi(result[0])
}

// FreezeTableOldWay - freeze all partitions in table one by one
// This way using for ClickHouse below v19.1
func (ch *ClickHouse) FreezeTableOldWay(table Table) error {
	var partitions []struct {
		PartitionID string `db:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s'", table.Database, table.Name)
	if err := ch.conn.Select(&partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for \"%s.%s\" with %v", table.Database, table.Name, err)
	}
	log.Printf("Freeze '%v.%v'", table.Database, table.Name)
	for _, item := range partitions {
		log.Printf("  partition '%v'", item.PartitionID)
		query := fmt.Sprintf(
			"ALTER TABLE `%v`.`%v` FREEZE PARTITION ID '%v';",
			table.Database,
			table.Name,
			item.PartitionID)
		if item.PartitionID == "all" {
			query = fmt.Sprintf(
				"ALTER TABLE `%v`.`%v` FREEZE PARTITION tuple();",
				table.Database,
				table.Name)
		}
		if _, err := ch.conn.Exec(query); err != nil {
			return fmt.Errorf("can't freeze partition '%s' on '%s.%s' with: %v", item.PartitionID, table.Database, table.Name, err)
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse sience v19.1
func (ch *ClickHouse) FreezeTable(table Table, name string) error {
	var query string

	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	log.Printf("version: %v\n", version)
	log.Printf("Freeze `%s`.`%s` with '%s'\n", table.Database, table.Name, name)
	//if version < 19001005 {
	if version < 19010001 { // 19.10
		return ch.FreezeTableOldWay(table)
	}
	switch {
	case len(name) == 0:
		query = fmt.Sprintf("ALTER TABLE `%v`.`%v` FREEZE;", table.Database, table.Name)
	default:
		query = fmt.Sprintf("ALTER TABLE `%v`.`%v` FREEZE WITH NAME '%v';", table.Database, table.Name, base58.Encode([]byte(name)))
	}
	if _, err := ch.conn.Exec(query); err != nil {
		return fmt.Errorf("can't freeze `%s`.`%s` with: %v", table.Database, table.Name, err)
	}
	return nil
}

// GetBackupTables - return list of backups of tables that can be restored
func (ch *ClickHouse) GetBackupTables(backupName string) (map[string]BackupTable, error) {
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return nil, err
	}
	backupShadowPath := filepath.Join(dataPath, "backup", backupName, "shadow")
	dbNum := 0
	tableNum := 1
	partNum := 2
	totalNum := 3
	if isClickhouseShadow(backupShadowPath) {
		dbNum = 2
		tableNum = 3
		partNum = 4
		totalNum = 5
	}
	fi, err := os.Stat(backupShadowPath)
	if err != nil {
		return nil, fmt.Errorf("can't get tables, %v", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("can't get tables, %s is not a dir", backupShadowPath)
	}

	result := make(map[string]BackupTable)
	err = filepath.Walk(backupShadowPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filePath = filepath.ToSlash(filePath) // fix fucking Windows slashes
			relativePath := strings.Trim(strings.TrimPrefix(filePath, backupShadowPath), "/")
			parts := strings.Split(relativePath, "/")
			if len(parts) != totalNum {
				return nil
			}
			partition := BackupPartition{
				Name: parts[partNum],
				Path: filePath,
			}
			tDB, _ := url.PathUnescape(parts[dbNum])
			tName, _ := url.PathUnescape(parts[tableNum])
			fullTableName := fmt.Sprintf("%s.%s", tDB, tName)
			if t, ok := result[fullTableName]; ok {
				t.Partitions = append(t.Partitions, partition)
				result[fullTableName] = t
				return nil
			}
			result[fullTableName] = BackupTable{
				Database:   tDB,
				Name:       tName,
				Partitions: []BackupPartition{partition},
			}
			return nil
		}
		return nil
	})
	return result, err
}

// Chown - set permission on file to clickhouse user
// This is necessary that the ClickHouse will be able to read parts files on restore
func (ch *ClickHouse) Chown(filename string) error {
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
	return os.Chown(filename, *ch.uid, *ch.gid)
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyData(table BackupTable) error {
	log.Printf("Prepare data for restoring `%s`.`%s`", table.Database, table.Name)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(table.Database), TablePathEncode(table.Name), "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range table.Partitions {
		detachedPath := filepath.Join(detachedParentDir, partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if os.IsNotExist(err) {
				// partition dir does not exist, creating
				os.MkdirAll(detachedPath, 0750)
			} else {
				return err
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' should be directory or absent", detachedPath)
		}
		ch.Chown(detachedPath)

		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix Windows slashes
			filename := strings.Trim(strings.TrimPrefix(filePath, partition.Path), "/")
			dstFilePath := filepath.Join(detachedPath, filename)
			if info.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !info.Mode().IsRegular() {
				log.Printf("'%s' is not a regular file, skipping.", filePath)
				return nil
			}
			if err := os.Link(filePath, dstFilePath); err != nil {
				return fmt.Errorf("failed to crete hard link '%s' -> '%s' with %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("error during filepath.Walk for partition '%s' with %v", partition.Path, err)
		}
	}
	return nil
}

func (ch *ClickHouse) CheckData(table BackupTable) error {
	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if version < 19010001 { // 19.10
		return nil
	}
	var ok []bool
	log.Printf("Check table `%s`.`%s`\n", table.Database, table.Name)
	query := fmt.Sprintf("CHECK TABLE `%s`.`%s`", table.Database, table.Name)
	err = ch.conn.Select(&ok, query)
	switch {
	case err != nil:
		return err
	case ok[0] != true:
		return errors.New("the data in the table is corrupted")
	default:
		return nil
	}
}

// AttachPatritions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPatritions(table BackupTable) error {
	for _, partition := range table.Partitions {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Name, partition.Name)
		log.Println(query)
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase - create ClickHouse database
func (ch *ClickHouse) CreateDatabase(database string) error {
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	_, err := ch.conn.Exec(createQuery)
	return err
}

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table RestoreTable) error {
	if _, err := ch.conn.Exec(fmt.Sprintf("USE `%s`", table.Database)); err != nil {
		return err
	}
	log.Printf("Create table `%s`.`%s`", table.Database, table.Table)
	if _, err := ch.conn.Exec(table.Query); err != nil {
		return err
	}
	return nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() *sqlx.DB {
	return ch.conn
}
