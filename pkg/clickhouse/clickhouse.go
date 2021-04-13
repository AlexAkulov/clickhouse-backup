package clickhouse

import (
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

	"github.com/AlexAkulov/clickhouse-backup/config"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

const (
	flashbackAvailableSinceChVersion int = 19150306
)

// ClickHouse - provide
type ClickHouse struct {
	Config *config.ClickHouseConfig
	conn   *sqlx.DB
	uid    *int
	gid    *int
}

// Table - ClickHouse table struct
type Table struct {
	Database string `db:"database" json:"database"`
	Name     string `db:"name" json:"table"`
	Skip     bool   `json:"skip"`
	Engine   string `json:"engine"`
}

// BackupPartition - struct representing Clickhouse partition
type BackupPartition struct {
	Partition                         string `json:"Partition"`
	Name                              string `json:"Name"`
	Path                              string `json:"Path"`
	HashOfAllFiles                    string `json:"hash_of_all_files"`
	HashOfUncompressedFiles           string `json:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string `json:"uncompressed_hash_of_compressed_files"`
	Active                            uint8  `json:"active"`
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

// Partition - partition info from system.parts
type Partition struct {
	Partition                         string `db:"partition"`
	Name                              string `db:"name"`
	Path                              string `db:"path"`
	HashOfAllFiles                    string `db:"hash_of_all_files"`
	HashOfUncompressedFiles           string `db:"hash_of_uncompressed_files"`
	UncompressedHashOfCompressedFiles string `db:"uncompressed_hash_of_compressed_files"`
	Active                            uint8  `db:"active"`
}

// PartDiff - Data part discrepancies infos
type PartDiff struct {
	BTable           BackupTable
	PartitionsAdd    []Partition
	PartitionsRemove []Partition
}

// Connect - establish connection to ClickHouse
func (ch *ClickHouse) Connect() error {
	timeout, err := time.ParseDuration(ch.Config.Timeout)
	if err != nil {
		return err
	}

	timeoutSeconds := fmt.Sprintf("%d", int(timeout.Seconds()))
	params := url.Values{}
	params.Add("username", ch.Config.Username)
	params.Add("password", ch.Config.Password)
	params.Add("database", "system")
	params.Add("receive_timeout", timeoutSeconds)
	params.Add("send_timeout", timeoutSeconds)
	if ch.Config.Secure {
		params.Add("secure", "true")
		params.Add("skip_verify", strconv.FormatBool(ch.Config.SkipVerify))
	}
	connectionString := fmt.Sprintf("tcp://%v:%v?%s", ch.Config.Host, ch.Config.Port, params.Encode())
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
	version, err := ch.GetVersion()
	if err != nil {
		return "", err
	}
	if version < 19015000 {
		return ch.getDataPathFromSystemSettings()
	}
	return ch.getDataPathFromSystemDisks()
}

func (ch *ClickHouse) getDataPathFromSystemSettings() (string, error) {
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

func (ch *ClickHouse) getDataPathFromSystemDisks() (string, error) {
	var result []struct {
		Path string `db:"path"`
	}
	query := "SELECT path FROM system.disks WHERE name == 'default' LIMIT 1;"
	if err := ch.conn.Select(&result, query); err != nil {
		return "/var/lib/clickhouse", err
	}
	return result[0].Path, nil
}

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

// GetTables - return slice of all tables suitable for backup
func (ch *ClickHouse) GetTables() ([]Table, error) {
	tables := make([]Table, 0)
	if err := ch.conn.Select(&tables, "SELECT database, name, engine FROM system.tables WHERE is_temporary = 0 AND engine LIKE '%MergeTree';"); err != nil {
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

// GetPartitions - return slice of all partitions for a table
func (ch *ClickHouse) GetPartitions(table Table) ([]Partition, error) {
	partitions := make([]Partition, 0)

	ver, errgv := ch.GetVersion()
	if errgv != nil {
		return nil, errgv
	}

	if ver >= flashbackAvailableSinceChVersion {
		if err := ch.conn.Select(&partitions, fmt.Sprintf("select partition, name, path, active, hash_of_all_files,hash_of_uncompressed_files,uncompressed_hash_of_compressed_files from system.parts where database='%s' and table='%s';", table.Database, table.Name)); err != nil {
			return nil, err
		}
	} else {
		if err := ch.conn.Select(&partitions, fmt.Sprintf("select partition, name, path, active from system.parts where database='%s' and table='%s';", table.Database, table.Name)); err != nil {
			return nil, err
		}
	}

	return partitions, nil
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion() (int, error) {
	var result []string
	q := "SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'"
	if err := ch.conn.Select(&result, q); err != nil {
		return 0, fmt.Errorf("can't get сlickHouse version: %v", err)
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
		return fmt.Errorf("can't get partitions for '%s.%s': %v", table.Database, table.Name, err)
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
			return fmt.Errorf("can't freeze partition '%s' on '%s.%s': %v", item.PartitionID, table.Database, table.Name, err)
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse sience v19.1
func (ch *ClickHouse) FreezeTable(table Table) error {
	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if version < 19001005 {
		return ch.FreezeTableOldWay(table)
	}
	if strings.HasPrefix(table.Engine, "Replicated") && ch.Config.SyncReplicatedTables {
		log.Printf("Sync '%s.%s'", table.Database, table.Name)
		query := fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`;", table.Database, table.Name)
		if _, err := ch.conn.Exec(query); err != nil {
			return fmt.Errorf("can't sync '%s.%s': %v", table.Database, table.Name, err)
		}
	}
	log.Printf("Freeze '%s.%s'", table.Database, table.Name)
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE;", table.Database, table.Name)
	if _, err := ch.conn.Exec(query); err != nil {
		return fmt.Errorf("can't freeze '%s.%s': %v", table.Database, table.Name, err)
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
	if IsClickhouseShadow(backupShadowPath) {
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

	var allpartsBackup map[string][]Partition
	// hashPath := path.Join(dataPath, "backup", backupName, hashfile)
	// log.Printf("Reading part hashes %s", hashPath)
	// bytes, err := ioutil.ReadFile(hashPath)
	// if err != nil {
	// 	log.Printf("Unable to read hash file %s", hashPath)
	// 	//return nil, fmt.Errorf("Unable to read hash file %s", hashPath)
	// } else {
	// 	err = json.Unmarshal(bytes, &allpartsBackup)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("issue occurred while reading hash file %s", hashPath)
	// 	}
	// }

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

			tDB, _ := url.PathUnescape(parts[dbNum])
			tName, _ := url.PathUnescape(parts[tableNum])
			fullTableName := fmt.Sprintf("%s.%s", tDB, tName)

			allparthash := allpartsBackup[fullTableName]
			var hoaf, houf, uhocf string
			for _, parthash := range allparthash {
				if parthash.Name == parts[partNum] {
					hoaf = parthash.HashOfAllFiles
					houf = parthash.HashOfUncompressedFiles
					uhocf = parthash.UncompressedHashOfCompressedFiles
				}
			}

			partition := BackupPartition{
				Name:                              parts[partNum],
				Path:                              filePath,
				HashOfAllFiles:                    hoaf,
				HashOfUncompressedFiles:           houf,
				UncompressedHashOfCompressedFiles: uhocf,
			}

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

// ComputePartitionsDelta - computes the data partitions to be added and removed between live and backup tables
func (ch *ClickHouse) ComputePartitionsDelta(restoreTables []BackupTable, liveTables []Table) ([]PartDiff, error) {
	var ftables []PartDiff
	var partitions []Partition
	log.Printf("Compute partitions discrepancies")
	for _, rtable := range restoreTables {
		var partsToAdd []Partition
		var partsToRemove []Partition
		for _, rpartition := range rtable.Partitions {
			bfound := false
			for _, liveTable := range liveTables {
				if liveTable.Database == rtable.Database && liveTable.Name == rtable.Name {
					livePartitions, _ := ch.GetPartitions(liveTable)
					for _, livePartition := range livePartitions {
						if livePartition.HashOfAllFiles == rpartition.HashOfAllFiles && livePartition.HashOfUncompressedFiles == rpartition.HashOfUncompressedFiles && livePartition.UncompressedHashOfCompressedFiles == rpartition.UncompressedHashOfCompressedFiles {
							bfound = true
							break
						}
					}
				}
			}
			if !bfound {
				partsToAdd = append(partsToAdd, Partition{Name: rpartition.Name, Path: rpartition.Path})
			}
		}

		for _, ltable := range liveTables {
			if ltable.Name == rtable.Name {
				partitions, _ = ch.GetPartitions(ltable)
				for _, livepart := range partitions {
					bfound := false
					for _, backuppart := range rtable.Partitions {
						if livepart.HashOfAllFiles == backuppart.HashOfAllFiles && livepart.HashOfUncompressedFiles == backuppart.HashOfUncompressedFiles && livepart.UncompressedHashOfCompressedFiles == backuppart.UncompressedHashOfCompressedFiles {
							bfound = true
							break
						}
					}
					if !bfound {
						partsToRemove = append(partsToRemove, livepart)
					}
				}
			}
		}
		log.Printf("[%s.%s] Backup data parts to attach : %v ", rtable.Database, rtable.Name, partsToAdd)
		log.Printf("[%s.%s] Live data parts to detach : %v ", rtable.Database, rtable.Name, partsToRemove)
		ftables = append(ftables, PartDiff{rtable, partsToAdd, partsToRemove})
	}
	log.Printf("Compute partitions discrepancies. Done")

	return ftables, nil
}

// CopyDataDiff - copy only partitions that will be attached to "detached" folder
func (ch *ClickHouse) CopyDataDiff(diff PartDiff) error {
	log.Printf("Prepare data for restoring '%s.%s'", diff.BTable.Database, diff.BTable.Name)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(diff.BTable.Database), TablePathEncode(diff.BTable.Name), "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range diff.PartitionsAdd {
		log.Printf("Processing partition %s (%s)", partition.Name, partition.Path)
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
				return fmt.Errorf("failed to crete hard link '%s' -> '%s': %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("error during filepath.Walk for partition '%s': %v", partition.Path, err)
		}
	}
	log.Printf("Prepare data for restoring '%s.%s'. DONE", diff.BTable.Database, diff.BTable.Name)
	return nil
}

// CopyData - copy partitions for specific table to detached folder
func (ch *ClickHouse) CopyData(table BackupTable) error {
	log.Printf("Prepare data for restoring '%s.%s'", table.Database, table.Name)
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(table.Database), TablePathEncode(table.Name), "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range table.Partitions {
		// log.Printf("partition name is %s (%s)", partition.Name, partition.Path)
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
				return fmt.Errorf("failed to crete hard link '%s' -> '%s': %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("error during filepath.Walk for partition '%s': %v", partition.Path, err)
		}
	}
	return nil
}

// ApplyPartitionsChanges - add/remove partitions to/from the live table
func (ch *ClickHouse) ApplyPartitionsChanges(table PartDiff) error {
	var query string
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return err
	}
	for _, partition := range table.PartitionsAdd {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.BTable.Database, table.BTable.Name, partition.Name)
		log.Println(query)
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}
	}

	if len(table.PartitionsRemove) > 0 {
		partList := make(map[string]struct{})

		for _, partition := range table.PartitionsRemove {
			log.Printf("Removing %s", partition.Path)

			//partitionName := partition.Name[:strings.IndexByte(partition.Name, '_')]
			partitionName := partition.Partition

			if _, ok := partList[partitionName]; !ok {
				partList[partitionName] = struct{}{}
			}
		}

		for partname := range partList {
			if partname != "tuple()" {
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` DETACH PARTITION '%s'", table.BTable.Database, table.BTable.Name, partname)
			} else {
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` DETACH PARTITION %s", table.BTable.Database, table.BTable.Name, partname)
			}
			log.Println(query)
			if _, err := ch.conn.Exec(query); err != nil {
				return err
			}
		}

		detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(table.BTable.Database), TablePathEncode(table.BTable.Name), "detached")

		for _, partition := range table.PartitionsRemove {
			detachedPath := filepath.Join(detachedParentDir, partition.Name)
			log.Printf("[%s.%s] Removing %s", table.BTable.Database, table.BTable.Name, detachedPath)
			e := os.RemoveAll(detachedPath)
			if e != nil {
				return e
			}
		}

		for partname := range partList {
			if partname == "all" {
				query = fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.BTable.Database, table.BTable.Name)
			} else {
				if partname != "tuple()" {
					query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION '%s'", table.BTable.Database, table.BTable.Name, partname)
				} else {
					query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION %s", table.BTable.Database, table.BTable.Name, partname)
				}
			}
			log.Println(query)
			if _, err := ch.conn.Exec(query); err != nil {
				return err
			}
		}
	}
	return nil
}

// AttachPartitions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPartitions(table BackupTable) error {
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
	createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ENGINE = Ordinary", database)
	_, err := ch.conn.Exec(createQuery)
	return err
}

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable bool) error {
	if _, err := ch.conn.Exec(fmt.Sprintf("USE `%s`", table.Database)); err != nil {
		return err
	}
	log.Printf("Create table '%s.%s'", table.Database, table.Name)
	if dropTable {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", table.Database, table.Name)
		if _, err := ch.conn.Exec(dropQuery); err != nil {
			return err
		}
	}
	if _, err := ch.conn.Exec(query); err != nil {
		return err
	}
	return nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() *sqlx.DB {
	return ch.conn
}

func IsClickhouseShadow(path string) bool {
	d, err := os.Open(path)
	if err != nil {
		return false
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return false
	}
	for _, name := range names {
		if name == "increment.txt" {
			continue
		}
		if _, err := strconv.Atoi(name); err != nil {
			return false
		}
	}
	return true
}

func TablePathEncode(str string) string {
	return strings.ReplaceAll(
		strings.ReplaceAll(url.PathEscape(str), ".", "%2E"), "-", "%2D")
}
