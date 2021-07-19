package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/utils"
	apexLog "github.com/apex/log"
	"github.com/mattn/go-shellwords"
	"github.com/otiai10/copy"
	"github.com/yargevad/filepathx"
)

// RestoreTables - slice of RestoreTable
type RestoreTables []metadata.TableMetadata

// Sort - sorting BackupTables slice orderly by name
func (rt RestoreTables) Sort(dropTable bool) {
	sort.Slice(rt, func(i, j int) bool {
		return getOrderByEngine(rt[i].Query, dropTable) < getOrderByEngine(rt[j].Query, dropTable)
	})
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(cfg *config.Config, backupName string, tablePattern string, schemaOnly, dataOnly, dropTable, doRestoreRBAC, doRestoreConfigs bool) error {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if backupName == "" {
		_ = PrintLocalBackups(cfg, "all")
		return fmt.Errorf("select backup for restore")
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()
	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	backupMetafileLocalPath := path.Join(defaultDataPath, "backup", backupName, "metadata.json")
	backupMetadataBody, err := ioutil.ReadFile(backupMetafileLocalPath)
	if err == nil {
		backupMetadata := metadata.BackupMetadata{}
		if err := json.Unmarshal(backupMetadataBody, &backupMetadata); err != nil {
			return err
		}
		if schemaOnly || (schemaOnly == dataOnly) {
			for _, database := range backupMetadata.Databases {
				if err := ch.CreateDatabaseFromQuery(database.Query); err != nil {
					return err
				}
			}
		}
		if len(backupMetadata.Tables) == 0 {
			apexLog.Infof("'%s' is empty backup, nothing to do", backupName)
			return nil
		}
	} else if !os.IsNotExist(err) { // Legacy backups don't contain metadata.json
		return err
	}

	if schemaOnly || (schemaOnly == dataOnly) {
		if err := RestoreSchema(ch, backupName, tablePattern, dropTable); err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		if err := RestoreData(cfg, ch, backupName, tablePattern); err != nil {
			return err
		}
	}
	if doRestoreRBAC {
		if err := restoreRBAC(ch, backupName); err != nil {
			return err
		}
	}
	if doRestoreConfigs {
		if err := restoreConfigs(ch, backupName); err != nil {
			return err
		}
	}

	if doRestoreConfigs || doRestoreRBAC {
		backupContainsDir := map[string]bool{
			"configs": false,
			"access":  false,
		}
		for prefix := range backupContainsDir {
			configRelatedBackupDir := path.Join(defaultDataPath, "backup", backupName, prefix)
			info, err := os.Stat(configRelatedBackupDir)
			backupContainsDir[prefix] = !os.IsNotExist(err) && info.IsDir()

			apexLog.Warnf("%s contains `%s` directory, so we need exec %s", backupName, prefix, ch.Config.RestartCommand)

		}
		if backupContainsDir["configs"] || backupContainsDir["access"] {
			cmd, err := shellwords.Parse(ch.Config.RestartCommand)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			apexLog.Infof("run %s", ch.Config.RestartCommand)
			var out []byte
			if len(cmd) > 1 {
				out, err = exec.CommandContext(ctx, cmd[0], cmd[1:]...).CombinedOutput()
			} else {
				out, err = exec.CommandContext(ctx, cmd[0]).CombinedOutput()
			}
			cancel()
			apexLog.Debug(string(out))
			return err
		}

	}
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})
	log.Info("done")
	return nil
}

// restoreRBAC - copy backup_name>/rbac folder to access_data_path
func restoreRBAC(ch *clickhouse.ClickHouse, backupName string) error {
	accessPath, err := ch.GetAccessManagementPath(nil)
	if err != nil {
		return err
	}
	if err = restoreBackupRelatedDir(ch, backupName, "access", accessPath); err != nil {
		return err
	}
	markFile := path.Join(accessPath, "need_rebuild_lists.mark")
	apexLog.Infof("create %s for properly rebuild RBAC after restart clickhouse-server", markFile)
	file, err := os.Create(markFile)
	if err != nil {
		return err
	}
	_ = file.Close()
	ch.Chown(markFile)
	listFilesPattern := path.Join(accessPath, "*.list")
	apexLog.Infof("remove %s for properly rebuild RBAC after restart clickhouse-server", listFilesPattern)
	if listFiles, err := filepathx.Glob(listFilesPattern); err != nil {
		return err
	} else {
		for _, f := range listFiles {
			if err := os.Remove(f); err != nil {
				return err
			}
		}
	}
	return nil
}

// restoreConfigs - copy backup_name/configs folder to /etc/clickhouse-server/
func restoreConfigs(ch *clickhouse.ClickHouse, backupName string) error {
	return restoreBackupRelatedDir(ch, backupName, "configs", ch.Config.ConfigDir)
}

func restoreBackupRelatedDir(ch *clickhouse.ClickHouse, backupName, backupPrefixDir, destinationDir string) error {
	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	srcBackupDir := path.Join(defaultDataPath, "backup", backupName, backupPrefixDir)
	info, err := os.Stat(srcBackupDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !os.IsNotExist(err) {
		if !info.IsDir() {
			return fmt.Errorf("%s is not a dir", srcBackupDir)
		}
		apexLog.Debugf("copy %s -> %s", srcBackupDir, destinationDir)
		copyOptions := copy.Options{OnDirExists: func(src, dest string) copy.DirExistsAction {
			return copy.Merge
		}}
		if err := copy.Copy(srcBackupDir, destinationDir, copyOptions); err != nil {
			return err
		}

		files, err := filepathx.Glob(path.Join(destinationDir, "**"))
		if err != nil {
			return err
		}
		files = append(files, destinationDir)
		for _, localFile := range files {
			if err := ch.Chown(localFile); err != nil {
				return err
			}
		}
	}
	return nil
}

// RestoreSchema - restore schemas matched by tablePattern from backupName
func RestoreSchema(ch *clickhouse.ClickHouse, backupName string, tablePattern string, dropTable bool) error {
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})

	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	metadataPath := path.Join(defaultDataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	if tablePattern == "" {
		tablePattern = "*"
	}
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern, dropTable)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}

	totalRetries := len(tablesForRestore)
	restoreRetries := 0
	var notRestoredTables RestoreTables
	var restoreErr error
	for restoreRetries < totalRetries {
		for _, schema := range tablesForRestore {
			// if metadata.json doesn't contains "databases", we will re-create tables with default engine
			if err = ch.CreateDatabase(schema.Database); err != nil {
				return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
			}
			//materialized views should restore via ATTACH
			schema.Query = strings.Replace(
				schema.Query, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
			)
			restoreErr = ch.CreateTable(clickhouse.Table{
				Database: schema.Database,
				Name:     schema.Table,
			}, schema.Query, dropTable)

			if restoreErr != nil {
				restoreRetries++
				if restoreRetries >= totalRetries {
					return fmt.Errorf(
						"can't create table `%s`.`%s`: %v after %d times, please check your schema depencncies",
						schema.Database, schema.Table, restoreErr, restoreRetries,
					)
				} else {
					log.Warnf(
						"can't create table '%s.%s': %v, will try again", schema.Database, schema.Table, restoreErr,
					)
				}
				notRestoredTables = append(notRestoredTables, schema)
			}
		}
		tablesForRestore = notRestoredTables
		if len(tablesForRestore) == 0 {
			break
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(cfg *config.Config, ch *clickhouse.ClickHouse, backupName string, tablePattern string) error {
	startRestore := time.Now()
	log := apexLog.WithFields(apexLog.Fields{
		"backup":    backupName,
		"operation": "restore",
	})
	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	if clickhouse.IsClickhouseShadow(path.Join(defaultDataPath, "backup", backupName, "shadow")) {
		return fmt.Errorf("backups created in v0.0.1 is not supported now")
	}
	backup, err := getLocalBackup(cfg, backupName)
	if err != nil {
		return fmt.Errorf("can't restore: %v", err)
	}
	var tablesForRestore RestoreTables
	if backup.Legacy {
		tablesForRestore, err = ch.GetBackupTablesLegacy(backupName)
	} else {
		metadataPath := path.Join(defaultDataPath, "backup", backupName, "metadata")
		tablesForRestore, err = parseSchemaPattern(metadataPath, tablePattern, false)
	}
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	log.Debugf("found %d tables with data in backup", len(tablesForRestore))
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	for _, t := range tablesForRestore {
		for disk := range t.Parts {
			if _, ok := diskMap[disk]; !ok {
				return fmt.Errorf("table '%s.%s' require disk '%s' that not found in clickhouse, you can add nonexistent disks to disk_mapping config", t.Database, t.Table, disk)
			}
		}
	}
	dstTablesMap := map[metadata.TableTitle]clickhouse.Table{}
	for i := range chTables {
		dstTablesMap[metadata.TableTitle{
			Database: chTables[i].Database,
			Table:    chTables[i].Name,
		}] = chTables[i]
	}

	var missingTables []string
	for _, restoreTable := range tablesForRestore {
		found := false
		for _, chTable := range chTables {
			if (restoreTable.Database == chTable.Database) && (restoreTable.Table == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("'%s.%s'", restoreTable.Database, restoreTable.Table))
		}
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}

	for _, table := range tablesForRestore {
		log := log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table))
		dstTableDataPaths := dstTablesMap[metadata.TableTitle{
			Database: table.Database,
			Table:    table.Table}].DataPaths
		if err := ch.CopyData(backupName, table, disks, dstTableDataPaths); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", table.Database, table.Table, err)
		}
		log.Debugf("copied data to 'detached'")
		if err := ch.AttachPartitions(table, disks); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", table.Database, table.Table, err)
		}
		log.Debugf("attached parts")
		log.Info("done")
	}
	log.WithField("duration", utils.HumanizeDuration(time.Since(startRestore))).Info("done")
	return nil
}
