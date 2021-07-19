// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/apex/log"
	"golang.org/x/mod/semver"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbNameAtomic = "_test.ДБ_atomic_"
const dbNameOrdinary = "_test.ДБ_ordinary_"

type TestDataStruct struct {
	Database           string
	DatabaseEngine     string
	Table              string
	Schema             string
	Rows               []map[string]interface{}
	Fields             []string
	OrderBy            string
	IsMaterializedView bool
	IsDictionary       bool
	SkipInsert         bool
}

var testData = []TestDataStruct{
	{
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  ".inner.table1",
		Schema: "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:14"), "Log": "One"},
			{"Date": toDate("2018-10-23"), "TimeStamp": toTS("2018-10-23 07:37:15"), "Log": "Two"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:16"), "Log": "Three"},
			{"Date": toDate("2018-10-24"), "TimeStamp": toTS("2018-10-24 07:37:17"), "Log": "Four"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:18"), "Log": "Five"},
			{"Date": toDate("2019-10-25"), "TimeStamp": toTS("2019-01-25 07:37:19"), "Log": "Six"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "2. Таблица №2",
		Schema: "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"id": uint64(1), "User": "Alice"},
			{"id": uint64(2), "User": "Bob"},
			{"id": uint64(3), "User": "John"},
			{"id": uint64(4), "User": "Frank"},
			{"id": uint64(5), "User": "Nancy"},
			{"id": uint64(6), "User": "Brandon"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "-table-3-",
		Schema: "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2018-10-23 07:37:14"), "Item": "One"},
			{"TimeStamp": toTS("2018-10-23 07:37:15"), "Item": "Two"},
			{"TimeStamp": toTS("2018-10-24 07:37:16"), "Item": "Three"},
			{"TimeStamp": toTS("2018-10-24 07:37:17"), "Item": "Four"},
			{"TimeStamp": toTS("2019-01-25 07:37:18"), "Item": "Five"},
			{"TimeStamp": toTS("2019-01-25 07:37:19"), "Item": "Six"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Table:  "table4",
		Schema: "(id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY id ORDER BY (id, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id", "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table2",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table3",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table4",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "1", "order_time": toTS("2010-01-01 00:00:00"), "amount": 1.0},
			{"order_id": "2", "order_time": toTS("2010-02-01 00:00:00"), "amount": 2.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "jbod",
		Schema: "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Table:  "jbod",
		Schema: "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Table:  "mv_src_table",
		Schema: "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		Table:          "mv_dst_table",
		Schema:         "(id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id",
		SkipInsert:     true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Table:              "mv_max_with_inner",
		Schema:             fmt.Sprintf("(id UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}','replica1') ORDER BY id AS SELECT max(id) AS id FROM `%s`.`mv_src_table`", dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Table:              "mv_max_with_dst",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT max(id) AS id FROM `%s`.mv_src_table", dbNameAtomic, dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:           dbNameAtomic,
		DatabaseEngine:     "Atomic",
		IsMaterializedView: true,
		Table:              "mv_min_with_nested_depencency",
		Schema:             fmt.Sprintf(" TO `%s`.`mv_dst_table` AS SELECT min(id) * 2 AS id FROM `%s`.mv_src_table", dbNameAtomic, dbNameAtomic),
		SkipInsert:         true,
		Rows: func() []map[string]interface{} {
			return []map[string]interface{}{
				{"id": uint64(0)},
				{"id": uint64(99)},
			}
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
	{
		Database:       dbNameAtomic,
		DatabaseEngine: "Atomic",
		IsDictionary:   true,
		Table:          "dict_example",
		Schema: fmt.Sprintf(
			" (id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) PRIMARY KEY id "+
				" SOURCE(CLICKHOUSE(host 'localhost' port 9000 database '%s' table 'table4' user 'default' password ''))"+
				" LAYOUT(HASHED()) LIFETIME(60)",
			dbNameAtomic),
		SkipInsert: true,
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 0; i < 100; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
}

var incrementData = []TestDataStruct{
	{
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  ".inner.table1",
		Schema: "(Date Date, TimeStamp DateTime, Log String) ENGINE = MergeTree(Date, (TimeStamp, Log), 8192)",
		Rows: []map[string]interface{}{
			{"Date": toDate("2019-10-26"), "TimeStamp": toTS("2019-01-26 07:37:19"), "Log": "Seven"},
		},
		Fields:  []string{"Date", "TimeStamp", "Log"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "2. Таблица №2",
		Schema: "(id UInt64, User String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"id": uint64(7), "User": "Alice"},
			{"id": uint64(8), "User": "Bob"},
			{"id": uint64(9), "User": "John"},
			{"id": uint64(10), "User": "Frank"},
		},
		Fields:  []string{"id", "User"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "-table-3-",
		Schema: "(TimeStamp DateTime, Item String, Date Date MATERIALIZED toDate(TimeStamp)) ENGINE = MergeTree() PARTITION BY Date ORDER BY TimeStamp SETTINGS index_granularity = 8192",
		Rows: []map[string]interface{}{
			{"TimeStamp": toTS("2019-01-26 07:37:18"), "Item": "Seven"},
			{"TimeStamp": toTS("2019-01-27 07:37:19"), "Item": "Eight"},
		},
		Fields:  []string{"TimeStamp", "Item"},
		OrderBy: "TimeStamp",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Table:  "table4",
		Schema: "(id UInt64, Col1 String, Col2 String, Col3 String, Col4 String, Col5 String) ENGINE = MergeTree PARTITION BY id ORDER BY (id, Col1, Col2, Col3, Col4, Col5) SETTINGS index_granularity = 8192",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 200; i < 220; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i), "Col1": "Text1", "Col2": "Text2", "Col3": "Text3", "Col4": "Text4", "Col5": "Text5"})
			}
			return result
		}(),
		Fields:  []string{"id", "Col1", "Col2", "Col3", "Col4", "Col5"},
		OrderBy: "id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table2",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMM(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table3",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(order_time) ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameOrdinary, DatabaseEngine: "Ordinary",
		Table:  "yuzhichang_table4",
		Schema: "(order_id String, order_time DateTime, amount Float64) ENGINE = MergeTree() ORDER BY (order_time, order_id)",
		Rows: []map[string]interface{}{
			{"order_id": "3", "order_time": toTS("2010-03-01 00:00:00"), "amount": 3.0},
			{"order_id": "4", "order_time": toTS("2010-04-01 00:00:00"), "amount": 4.0},
		},
		Fields:  []string{"order_id", "order_time", "amount"},
		OrderBy: "order_id",
	}, {
		Database: dbNameAtomic, DatabaseEngine: "Atomic",
		Table:  "jbod",
		Schema: "(id UInt64) Engine=MergeTree ORDER BY id SETTINGS storage_policy = 'jbod'",
		Rows: func() []map[string]interface{} {
			var result []map[string]interface{}
			for i := 100; i < 200; i++ {
				result = append(result, map[string]interface{}{"id": uint64(i)})
			}
			return result
		}(),
		Fields:  []string{"id"},
		OrderBy: "id",
	},
}

func init() {
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log.SetLevelFromString(logLevel)
}

func TestIntegrationS3(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	testCommon(t)
}

func TestIntegrationGCS(t *testing.T) {
	if os.Getenv("GCS_TESTS") == "" || os.Getenv("TRAVIS_PULL_REQUEST") != "false" {
		t.Skip("Skipping GCS integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-gcs.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("clickhouse", "apt-get", "-y", "update"))
	r.NoError(dockerExec("clickhouse", "apt-get", "-y", "install", "ca-certificates"))
	testCommon(t)
}

func TestIntegrationAzure(t *testing.T) {
	if os.Getenv("AZURE_TESTS") == "" || os.Getenv("TRAVIS_PULL_REQUEST") != "false" {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	r := require.New(t)
	r.NoError(dockerCP("config-azblob.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("clickhouse", "apt-get", "-y", "update"))
	r.NoError(dockerExec("clickhouse", "apt-get", "-y", "install", "ca-certificates"))
	testCommon(t)
}

func TestIntegrationSFTPAuthPassword(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-sftp-auth-password.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	testCommon(t)
}

func TestIntegrationSFTPAuthKey(t *testing.T) {
	r := require.New(t)
	r.NoError(dockerCP("config-sftp-auth-key.yaml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa", "clickhouse:/id_rsa"))
	r.NoError(dockerExec("clickhouse", "cp", "-vf", "/id_rsa", "/tmp/id_rsa"))
	r.NoError(dockerExec("clickhouse", "chmod", "-v", "0600", "/tmp/id_rsa"))

	r.NoError(dockerCP("sftp/clickhouse-backup_rsa.pub", "sshd:/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chown", "-v", "root:root", "/root/.ssh/authorized_keys"))
	r.NoError(dockerExec("sshd", "chmod", "-v", "0600", "/root/.ssh/authorized_keys"))

	testCommon(t)
}

func TestSyncReplicaTimeout(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "19.11") == -1 {
		t.Skipf("Test skipped, SYNC REPLICA ignore receive_timeout for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r)
	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))

	for _, table := range []string{"repl1", "repl2"} {
		query := "DROP TABLE IF EXISTS default." + table
		if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.3") == 1 {
			query += " NO DELAY"
		}
		ch.queryWithNoError(r, query)
	}

	ch.queryWithNoError(r, "CREATE TABLE default.repl1 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/default/repl','repl1') ORDER BY tuple()")
	ch.queryWithNoError(r, "CREATE TABLE default.repl2 (v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/default/repl','repl2') ORDER BY tuple()")

	ch.queryWithNoError(r, "INSERT INTO default.repl1 SELECT number FROM numbers(10)")

	ch.queryWithNoError(r, "SYSTEM STOP REPLICATED SENDS default.repl1")
	ch.queryWithNoError(r, "SYSTEM STOP FETCHES default.repl2")

	ch.queryWithNoError(r, "INSERT INTO default.repl1 SELECT number FROM numbers(100)")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--tables=default.repl*", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_not_synced_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_not_synced_backup"))

	ch.queryWithNoError(r, "SYSTEM START REPLICATED SENDS default.repl1")
	ch.queryWithNoError(r, "SYSTEM START FETCHES default.repl2")

	ch.chbackup.Close()

}

func TestDoRestoreRBAC(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.4") == -1 {
		t.Skipf("Test skipped, RBAC not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)

	ch.connectWithWait(r)

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE  IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY IF EXISTS test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE IF EXISTS test_rbac")
	ch.queryWithNoError(r, "DROP USER IF EXISTS test_rbac")

	log.Info("create RBAC related objects")
	ch.queryWithNoError(r, "CREATE SETTINGS PROFILE test_rbac SETTINGS max_execution_time=60")
	ch.queryWithNoError(r, "CREATE ROLE test_rbac SETTINGS PROFILE 'test_rbac'")
	ch.queryWithNoError(r, "CREATE USER test_rbac IDENTIFIED BY 'test_rbac' DEFAULT ROLE test_rbac")
	ch.queryWithNoError(r, "CREATE QUOTA test_rbac KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO test_rbac")
	ch.queryWithNoError(r, "CREATE ROW POLICY test_rbac ON default.test_rbac USING 1=1 AS RESTRICTIVE TO test_rbac")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--backup-rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	log.Info("drop all RBAC related objects after backup")
	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))
	startTime := time.Now()
	log.Info("restore")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_rbac_backup"))
	endTime := time.Now()
	waitBeforeRestore := 65*time.Second - endTime.Sub(startTime)
	log.Infof("wait %f to rebuild access/*.list", waitBeforeRestore.Seconds())
	time.Sleep(waitBeforeRestore)

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--do-restore-rbac", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	// we can't restart clickhouse inside container, we need restart container
	ch.chbackup.Close()
	_, err := execCmdOut("docker", "restart", "clickhouse")
	r.NoError(err)
	ch.connectWithWait(r)

	r.NoError(dockerExec("clickhouse", "ls", "-lah", "/var/lib/clickhouse/access"))

	rbacTypes := map[string]string{
		"PROFILES": "test_rbac",
		"QUOTAS":   "test_rbac",
		"POLICIES": "test_rbac ON default.test_rbac",
		"ROLES":    "test_rbac",
		"USERS":    "test_rbac",
	}
	for rbacType, expectedValue := range rbacTypes {
		rbacRows := []string{}
		ch.chbackup.Select(&rbacRows, fmt.Sprintf("SHOW %s", rbacType))
		found := false
		for _, row := range rbacRows {
			if row == expectedValue {
				found = true
				break
			}
		}
		if !found {
			r.NoError(dockerExec("clickhouse", "cat", "/var/log/clickhouse-server/clickhouse-server.log"))
		}
		r.Contains(rbacRows, expectedValue, "Invalid result for SHOW %s", rbacType)
	}
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_rbac_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_rbac_backup"))

	ch.queryWithNoError(r, "DROP SETTINGS PROFILE test_rbac")
	ch.queryWithNoError(r, "DROP QUOTA test_rbac")
	ch.queryWithNoError(r, "DROP ROW POLICY test_rbac ON default.test_rbac")
	ch.queryWithNoError(r, "DROP ROLE test_rbac")
	ch.queryWithNoError(r, "DROP USER test_rbac")

	ch.chbackup.Close()

}

func TestDoRestoreConfigs(t *testing.T) {
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "1.1.54391") == -1 {
		t.Skipf("Test skipped, users.d is not available for %s version", os.Getenv("CLICKHOUSE_VERSION"))
	}
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r)
	ch.queryWithNoError(r, "DROP TABLE IF EXISTS default.test_rbac")
	ch.queryWithNoError(r, "CREATE TABLE default.test_rbac (v UInt64) ENGINE=MergeTree() ORDER BY tuple()")

	r.NoError(dockerCP("config-s3.yml", "clickhouse:/etc/clickhouse-backup/config.yml"))
	r.NoError(dockerExec("clickhouse", "bash", "-c", "echo '<yandex><profiles><default><empty_result_for_aggregation_by_empty_set>1</empty_result_for_aggregation_by_empty_set></default></profiles></yandex>' > /etc/clickhouse-server/users.d/test_config.xml"))

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "--do-backup-configs", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))

	ch.chbackup.Close()
	time.Sleep(2 * time.Second)
	ch.connectWithWait(r)

	settings := []string{}
	r.NoError(ch.chbackup.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"1"}, settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "rm", "-rfv", "/etc/clickhouse-server/users.d/test_config.xml"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_configs_backup"))

	ch.chbackup.Close()
	time.Sleep(2 * time.Second)
	ch.connectWithWait(r)

	settings = []string{}
	r.NoError(ch.chbackup.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"0"}, settings, "expect empty_result_for_aggregation_by_empty_set=0")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "--do-restore-configs", "test_configs_backup"))

	ch.chbackup.Close()
	time.Sleep(2 * time.Second)
	ch.connectWithWait(r)

	settings = []string{}
	r.NoError(ch.chbackup.Select(&settings, "SELECT value FROM system.settings WHERE name='empty_result_for_aggregation_by_empty_set'"))
	r.Equal([]string{"1"}, settings, "expect empty_result_for_aggregation_by_empty_set=1")

	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_configs_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_configs_backup"))

	ch.chbackup.Close()
}

func testCommon(t *testing.T) {
	var out string
	var err error

	time.Sleep(time.Second * 5)
	ch := &TestClickHouse{}
	r := require.New(t)
	ch.connectWithWait(r)

	log.Info("Clean before start")
	_ = dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_backup")
	_ = dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup")
	_ = dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "increment")
	_ = dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "increment")
	dropAllDatabases(r, ch)
	generateTestData(ch, r)

	log.Info("Create backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "test_backup"))
	log.Info("Generate increment test data")
	for _, data := range incrementData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
	time.Sleep(time.Second * 5)
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "create", "increment"))

	log.Info("Upload")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "test_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "upload", "increment", "--diff-from", "test_backup"))

	dropAllDatabases(r, ch)

	out, err = dockerExecOut("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(5, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect one backup exists in backup directory")
	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup"))
	out, err = dockerExecOut("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup")
	r.NoError(err)
	r.Equal(4, len(strings.Split(strings.Trim(out, " \t\r\n"), "\n")), "expect no backup exists in backup directory")

	log.Info("Download")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "test_backup"))

	log.Info("Restore schema")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", "test_backup"))

	log.Info("Restore data")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--data", "test_backup"))

	log.Info("Full restore with rm")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--rm", "test_backup"))

	log.Info("Check data")
	for i := range testData {
		if isTableSkip(ch, testData[i], true) {
			continue
		}
		r.NoError(ch.checkData(t, testData[i]))
	}
	// test increment
	dropAllDatabases(r, ch)
	r.NoError(dockerExec("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup"))
	log.Info("Delete backup")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "increment"))
	r.NoError(dockerExec("clickhouse", "ls", "-lha", "/var/lib/clickhouse/backup"))

	log.Info("Download increment")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "download", "increment"))

	log.Info("Restore")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "restore", "--schema", "--data", "increment"))

	log.Info("Check increment data")
	for i := range testData {
		if isTableSkip(ch, testData[i], true) || testData[i].IsDictionary {
			continue
		}
		testDataItem := testData[i]
		for _, incrementDataItem := range incrementData {
			if testDataItem.Database == incrementDataItem.Database && testDataItem.Table == incrementDataItem.Table {
				testDataItem.Rows = append(testDataItem.Rows, incrementDataItem.Rows...)
			}
		}
		r.NoError(ch.checkData(t, testDataItem))
	}

	log.Info("Clean")
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "test_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "test_backup"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "remote", "increment"))
	r.NoError(dockerExec("clickhouse", "clickhouse-backup", "delete", "local", "increment"))

	ch.chbackup.Close()
}

func generateTestData(ch *TestClickHouse, r *require.Assertions) {
	log.Info("Generate test data")
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestSchema(data))
	}
	for _, data := range testData {
		if isTableSkip(ch, data, false) {
			continue
		}
		r.NoError(ch.createTestData(data))
	}
}

func dropAllDatabases(r *require.Assertions, ch *TestClickHouse) {
	log.Info("Drop all databases")
	r.NoError(ch.dropDatabase(dbNameOrdinary))
	r.NoError(ch.dropDatabase(dbNameAtomic))
}

type TestClickHouse struct {
	chbackup *clickhouse.ClickHouse
}

func (ch *TestClickHouse) connectWithWait(r *require.Assertions) {
	for i := 1; i < 11; i++ {
		err := ch.connect()
		if i == 10 {
			r.NoError(err)
		}
		if err != nil {
			log.Warnf("clickhouse not ready %v, wait %d seconds", err, i*2)
			r.NoError(execCmd("docker", "ps", "-a"))
			time.Sleep(time.Second * time.Duration(i*2))
		}
	}
}

func (ch *TestClickHouse) connect() error {
	ch.chbackup = &clickhouse.ClickHouse{
		Config: &config.ClickHouseConfig{
			Host:    "127.0.0.1",
			Port:    9000,
			Timeout: "5m",
		},
	}
	return ch.chbackup.Connect()
}

func (ch *TestClickHouse) createTestSchema(data TestDataStruct) error {
	// 20.8 doesn't respect DROP TABLE .. NO DELAY, so Atomic works but --rm is not applicable
	if compareVersion(os.Getenv("CLICKHOUSE_VERSION"), "20.8") == 1 {
		if err := ch.chbackup.CreateDatabaseWithEngine(data.Database, data.DatabaseEngine); err != nil {
			return err
		}
	} else {
		if err := ch.chbackup.CreateDatabase(data.Database); err != nil {
			return err
		}
	}
	createSQL := "CREATE "
	if data.IsMaterializedView {
		createSQL += " MATERIALIZED VIEW "
	} else if data.IsDictionary {
		createSQL += " DICTIONARY "
	} else {
		createSQL += " TABLE "
	}
	createSQL += fmt.Sprintf(" IF NOT EXISTS `%s`.`%s` ", data.Database, data.Table)
	createSQL += data.Schema
	// old 1.x clickhouse versions doesn't contains {table} and {database} macros
	var isMacrosExists []int
	if err := ch.chbackup.Select(&isMacrosExists, "SELECT count() FROM system.functions WHERE name='getMacro'"); err != nil {
		return err
	}
	if len(isMacrosExists) == 0 || isMacrosExists[0] == 0 {
		createSQL = strings.Replace(createSQL, "{table}", data.Table, -1)
		createSQL = strings.Replace(createSQL, "{database}", data.Database, -1)
	}
	err := ch.chbackup.CreateTable(
		clickhouse.Table{
			Database: data.Database,
			Name:     data.Table,
		},
		createSQL,
		false,
	)
	return err
}

func (ch *TestClickHouse) createTestData(data TestDataStruct) error {
	if data.SkipInsert {
		return nil
	}
	tx, err := ch.chbackup.GetConn().Beginx()
	if err != nil {
		return fmt.Errorf("can't begin transaction: %v", err)
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (:%s)",
		data.Database,
		data.Table,
		strings.Join(data.Fields, ","),
		strings.Join(data.Fields, ",:"),
	)
	log.Debug(insertSQL)
	statement, err := tx.PrepareNamed(insertSQL)
	if err != nil {
		return fmt.Errorf("can't prepare %s: %v", insertSQL, err)
	}
	defer func() {
		err = statement.Close()
		if err != nil {
			log.Warnf("can't close SQL statement")
		}
	}()

	for _, row := range data.Rows {
		log.Debugf("%#v", row)
		if _, err := statement.Exec(row); err != nil {
			return fmt.Errorf("can't add insert to transaction: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("can't commit transaction: %v", err)
	}
	return nil
}

func (ch *TestClickHouse) dropDatabase(database string) (err error) {
	var isAtomic bool
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database)
	if isAtomic, err = ch.chbackup.IsAtomic(database); isAtomic {
		dropDatabaseSQL += " SYNC"
	} else if err != nil {
		return err
	}
	_, err = ch.chbackup.Query(dropDatabaseSQL)
	return err
}

func (ch *TestClickHouse) checkData(t *testing.T, data TestDataStruct) error {
	assert.NotNil(t, data.Rows)
	log.Infof("Check '%d' rows in '%s.%s'\n", len(data.Rows), data.Database, data.Table)
	selectSQL := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY %s", data.Database, data.Table, data.OrderBy)
	log.Debug(selectSQL)
	rows, err := ch.chbackup.GetConn().Queryx(selectSQL)
	if err != nil {
		return err
	}
	var result []map[string]interface{}
	for rows.Next() {
		row := map[string]interface{}{}
		if rows.MapScan(row) != nil {
			return err
		}
		result = append(result, row)
	}
	assert.Equal(t, len(data.Rows), len(result))
	for i := range data.Rows {
		//goland:noinspection GoNilness
		assert.EqualValues(t, data.Rows[i], result[i])
	}
	return nil
}

func (ch *TestClickHouse) queryWithNoError(r *require.Assertions, query string, args ...interface{}) {
	_, err := ch.chbackup.Query(query, args...)
	r.NoError(err)
}

func dockerExec(container string, cmd ...string) error {
	out, err := dockerExecOut(container, cmd...)
	log.Info(out)
	return err
}

func dockerExecOut(container string, cmd ...string) (string, error) {
	dcmd := []string{"exec", container}
	dcmd = append(dcmd, cmd...)
	return execCmdOut("docker", dcmd...)
}

func execCmd(cmd string, args ...string) error {
	out, err := execCmdOut(cmd, args...)
	log.Info(string(out))
	return err
}

func execCmdOut(cmd string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	log.Infof("%s %s", cmd, strings.Join(args, " "))
	out, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	cancel()
	return string(out), err
}

func dockerCP(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	dcmd := []string{"cp", src, dst}
	log.Infof("docker %s", strings.Join(dcmd, " "))
	out, err := exec.CommandContext(ctx, "docker", dcmd...).CombinedOutput()
	log.Info(string(out))
	cancel()
	return err
}

func toDate(s string) time.Time {
	result, _ := time.Parse("2006-01-02", s)
	return result
}

func toTS(s string) time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05", s)
	return result
}

func isTableSkip(ch *TestClickHouse, data TestDataStruct, dataExists bool) bool {
	if data.IsDictionary && os.Getenv("COMPOSE_FILE") != "docker-compose.yml" && dataExists {
		var dictEngines []string
		dictSQL := fmt.Sprintf(
			"SELECT engine FROM system.tables WHERE name='%s' AND database='%s'",
			data.Table, data.Database,
		)
		_ = ch.chbackup.Select(&dictEngines, dictSQL)
		return len(dictEngines) == 0
	}
	return os.Getenv("COMPOSE_FILE") == "docker-compose.yml" && (data.Table == "jbod" || data.IsDictionary)
}

func compareVersion(v1, v2 string) int {
	v1 = "v" + v1
	v2 = "v" + v2
	if strings.Count(v1, ".") > 2 {
		v1 = strings.Join(strings.Split(v1, ".")[0:2], ".")
	}
	return semver.Compare(v1, v2)
}
