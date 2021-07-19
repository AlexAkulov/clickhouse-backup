package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/backup"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"

	apexLog "github.com/apex/log"
	"github.com/google/shlex"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli"
)

const (
	// APITimeFormat - clickhouse compatibility time format
	APITimeFormat  = "2006-01-02 15:04:05"
	InProgressText = "in progress"
)

type APIServer struct {
	c                       *cli.App
	configPath              string
	config                  *config.Config
	server                  *http.Server
	restart                 chan struct{}
	status                  *AsyncStatus
	metrics                 Metrics
	routes                  []string
	clickhouseBackupVersion string
}

type AsyncStatus struct {
	commands []ActionRow
	sync.RWMutex
}

type ActionRow struct {
	Command string `json:"command"`
	Status  string `json:"status"`
	Start   string `json:"start,omitempty"`
	Finish  string `json:"finish,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (status *AsyncStatus) start(command string) {
	status.Lock()
	defer status.Unlock()
	status.commands = append(status.commands, ActionRow{
		Command: command,
		Start:   time.Now().Format(APITimeFormat),
		Status:  InProgressText,
	})
}

func (status *AsyncStatus) inProgress() bool {
	status.RLock()
	defer status.RUnlock()
	n := len(status.commands) - 1
	if n < 0 {
		return false
	}
	return status.commands[n].Status == InProgressText
}

func (status *AsyncStatus) stop(err error) {
	status.Lock()
	defer status.Unlock()
	n := len(status.commands) - 1
	s := "success"
	if err != nil {
		s = "error"
		status.commands[n].Error = err.Error()
	}
	status.commands[n].Status = s
	status.commands[n].Finish = time.Now().Format(APITimeFormat)
}

func (status *AsyncStatus) status() []ActionRow {
	status.RLock()
	defer status.RUnlock()
	return status.commands
}

var (
	ErrAPILocked = errors.New("another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(c *cli.App, configPath string, clickhouseBackupVersion string) error {
	var (
		cfg *config.Config
		err error
	)
	apexLog.Debug("Wait for ClickHouse")
	for {
		cfg, err = config.LoadConfig(configPath)
		if err != nil {
			apexLog.Error(err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		ch := clickhouse.ClickHouse{
			Config: &cfg.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			apexLog.Error(err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		ch.GetConn().Close()
		break
	}
	api := APIServer{
		c:                       c,
		configPath:              configPath,
		config:                  cfg,
		restart:                 make(chan struct{}),
		status:                  &AsyncStatus{},
		clickhouseBackupVersion: clickhouseBackupVersion,
	}
	if cfg.API.CreateIntegrationTables {
		apexLog.Infof("Create integration tables")
		if err := api.CreateIntegrationTables(); err != nil {
			apexLog.Error(err.Error())
		}
	}
	api.metrics = setupMetrics()
	apexLog.Debug("Update last backup size metrics")
	if err := api.updateSizeOfLastBackup(); err != nil {
		apexLog.Error(err.Error())
	}
	apexLog.Infof("Starting API server on %s", api.config.API.ListenAddr)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)
	if err := api.Restart(); err != nil {
		return err
	}

	for {
		select {
		case <-api.restart:
			if err := api.Restart(); err != nil {
				apexLog.Errorf("Failed to restarting API server: %v", err)
				continue
			}
			apexLog.Infof("Reloaded by HTTP")
		case <-sighup:
			if err := api.Restart(); err != nil {
				apexLog.Errorf("Failed to restarting API server: %v", err)
				continue
			}
			apexLog.Info("Reloaded by SIGHUP")
		case <-sigterm:
			apexLog.Info("Stopping API server")
			return api.server.Close()
		}
	}
}

func (api *APIServer) Restart() error {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		return err
	}
	api.config = cfg
	server := api.setupAPIServer()
	if api.server != nil {
		api.server.Close()
	}
	api.server = server
	if api.config.API.Secure {
		go api.server.ListenAndServeTLS(api.config.API.CertificateFile, api.config.API.PrivateKeyFile)
		return nil
	}
	go api.server.ListenAndServe()
	return nil
}

// setupAPIServer - resister API routes
func (api *APIServer) setupAPIServer() *http.Server {
	r := mux.NewRouter()
	r.Use(api.basicAuthMidleware)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "", fmt.Errorf("404 Not Found"))
	})
	r.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "", fmt.Errorf("405 Method Not Allowed"))
	})

	r.HandleFunc("/", api.httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/list", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/create", api.httpCreateHandler).Methods("POST")
	r.HandleFunc("/backup/upload/{name}", api.httpUploadHandler).Methods("POST")
	r.HandleFunc("/backup/download/{name}", api.httpDownloadHandler).Methods("POST")
	r.HandleFunc("/backup/restore/{name}", api.httpRestoreHandler).Methods("POST")
	r.HandleFunc("/backup/delete/{where}/{name}", api.httpDeleteHandler).Methods("POST")
	r.HandleFunc("/backup/status", api.httpBackupStatusHandler).Methods("GET")

	r.HandleFunc("/backup/actions", api.actionsLog).Methods("GET")
	r.HandleFunc("/backup/actions", api.actions).Methods("POST")

	var routes []string
	r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		t, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		routes = append(routes, t)
		return nil
	})
	api.routes = routes
	registerMetricsHandlers(r, api.config.API.EnableMetrics, api.config.API.EnablePprof)
	srv := &http.Server{
		Addr:    api.config.API.ListenAddr,
		Handler: r,
	}
	return srv
}

func (api *APIServer) basicAuthMidleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		query := r.URL.Query()
		if u, exist := query["user"]; exist {
			user = u[0]
		}
		if p, exist := query["pass"]; exist {
			pass = p[0]
		}
		if (user != api.config.API.Username) || (pass != api.config.API.Password) {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"Provide username and password\"")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("401 Unauthorized\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String) ENGINE=URL('http://127.0.0.1:7171/backup/actions?user=user&pass=pass', JSONEachRow)
// INSERT INTO system.backup_actions (command) VALUES ('create backup_name')
// INSERT INTO system.backup_actions (command) VALUES ('upload backup_name')
func (api *APIServer) actions(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "", err)
		return
	}
	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "", fmt.Errorf("empty request"))
		return
	}
	lines := bytes.Split(body, []byte("\n"))
	for _, line := range lines {
		row := ActionRow{}
		if err := json.Unmarshal(line, &row); err != nil {
			writeError(w, http.StatusBadRequest, "", err)
			return
		}
		apexLog.Infof(row.Command)
		args, err := shlex.Split(row.Command)
		if err != nil {
			writeError(w, http.StatusBadRequest, "", err)
			return
		}
		command := args[0]
		switch command {
		case "create", "restore", "upload", "download", "create_remote", "restore_remote":
			if api.status.inProgress() {
				apexLog.Info(ErrAPILocked.Error())
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			go func() {
				start := time.Now()
				api.metrics.LastStart[command].Set(float64(start.Unix()))
				defer func() {
					api.metrics.LastDuration[command].Set(float64(time.Since(start).Nanoseconds()))
					api.metrics.LastFinish[command].Set(float64(time.Now().Unix()))
				}()

				api.status.start(row.Command)
				err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
				defer api.status.stop(err)
				if err != nil {
					api.metrics.FailedCounter[command].Inc()
					api.metrics.LastStatus[command].Set(0)
					apexLog.Error(err.Error())
					return
				}
				if err := api.updateSizeOfLastBackup(); err != nil {
					apexLog.Errorf("update size: %v", err)
				}
				api.metrics.SuccessfulCounter[command].Inc()
				api.metrics.LastStatus[command].Set(1)
			}()
			sendJSONEachRow(w, http.StatusCreated, struct {
				Status    string `json:"status"`
				Operation string `json:"operation"`
			}{
				Status:    "acknowledged",
				Operation: row.Command,
			})
			return
		case "delete":
			if api.status.inProgress() {
				apexLog.Info(ErrAPILocked.Error())
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			api.status.start(row.Command)
			err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
			defer api.status.stop(err)
			if err != nil {
				writeError(w, http.StatusBadRequest, row.Command, err)
				apexLog.Error(err.Error())
				return
			}
			apexLog.Info("OK")
			if err := api.updateSizeOfLastBackup(); err != nil {
				apexLog.Errorf("update size: %v", err)
			}
			sendJSONEachRow(w, http.StatusCreated, struct {
				Status    string `json:"status"`
				Operation string `json:"operation"`
			}{
				Status:    "ok",
				Operation: row.Command,
			})
			return
		default:
			writeError(w, http.StatusBadRequest, row.Command, fmt.Errorf("unknown command"))
			return
		}
	}
}

func (api *APIServer) actionsLog(w http.ResponseWriter, _ *http.Request) {
	sendJSONEachRow(w, http.StatusOK, api.status.status())
}

// httpRootHandler - display API index
func (api *APIServer) httpRootHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")

	fmt.Fprintln(w, "Documentation: https://github.com/AlexAkulov/clickhouse-backup#api-configuration")
	for _, r := range api.routes {
		fmt.Fprintln(w, r)
	}
}

// httpTablesHandler - displaylist of tables
func (api *APIServer) httpTablesHandler(w http.ResponseWriter, _ *http.Request) {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	tables, err := backup.GetTables(*cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "tables", err)
		return
	}
	sendJSONEachRow(w, http.StatusOK, tables)
}

// httpTablesHandler - display list of all backups stored locally and remotely
// CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String, desc String) ENGINE=URL('http://127.0.0.1:7171/backup/list?user=user&pass=pass', JSONEachRow)
// ??? INSERT INTO system.backup_list (name,location) VALUES ('backup_name', 'remote') - upload backup
// ??? INSERT INTO system.backup_list (name) VALUES ('backup_name') - create backup
func (api *APIServer) httpListHandler(w http.ResponseWriter, _ *http.Request) {
	type backupJSON struct {
		Name     string `json:"name"`
		Created  string `json:"created"`
		Size     int64  `json:"size,omitempty"`
		Location string `json:"location"`
		Desc     string `json:"desc"`
	}
	backupsJSON := make([]backupJSON, 0)
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	localBackups, err := backup.GetLocalBackups(cfg)
	if err != nil && !os.IsNotExist(err) {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	for _, b := range localBackups {
		description := b.DataFormat
		if b.Legacy {
			description = "old-format"
		}
		if b.Broken != "" {
			description = b.Broken
		}
		backupsJSON = append(backupsJSON, backupJSON{
			Name:     b.BackupName,
			Created:  b.CreationDate.Format(APITimeFormat),
			Size:     b.DataSize + b.MetadataSize,
			Location: "local",
			Desc:     description,
		})
	}
	if cfg.General.RemoteStorage != "none" {
		remoteBackups, err := backup.GetRemoteBackups(cfg)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for _, b := range remoteBackups {
			description := b.DataFormat
			if b.Legacy {
				description = "old-format"
			}
			if b.Broken != "" {
				description = b.Broken
			}
			backupsJSON = append(backupsJSON, backupJSON{
				Name:     b.BackupName,
				Created:  b.CreationDate.Format(APITimeFormat),
				Size:     b.DataSize + b.MetadataSize,
				Location: "remote",
				Desc:     description,
			})
		}
	}
	sendJSONEachRow(w, http.StatusOK, backupsJSON)
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "create", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	tablePattern := ""
	backupName := backup.NewBackupName()
	schemaOnly := false
	rbac := false
	backupConfig := false
	fullCommand := "create"
	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tp)
	}
	if schema, exist := query["schema"]; exist {
		schemaOnly, _ = strconv.ParseBool(schema[0])
		fullCommand = fmt.Sprintf("%s --schema", fullCommand)
	}
	if schema, exist := query["rbac"]; exist {
		rbac, _ = strconv.ParseBool(schema[0])
		fullCommand = fmt.Sprintf("%s --rbac", fullCommand)
	}
	if schema, exist := query["backupConfig"]; exist {
		backupConfig, _ = strconv.ParseBool(schema[0])
		fullCommand = fmt.Sprintf("%s --backup-config", fullCommand)
	}
	if name, exist := query["name"]; exist {
		backupName = name[0]
		fullCommand = fmt.Sprintf("%s %s", fullCommand, backupName)
	}

	go func() {
		api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["create"].Set(float64(start.Unix()))
		defer api.metrics.LastDuration["create"].Set(float64(time.Since(start).Nanoseconds()))
		defer api.metrics.LastFinish["create"].Set(float64(time.Now().Unix()))
		err := backup.CreateBackup(cfg, backupName, tablePattern, schemaOnly, rbac, backupConfig, api.clickhouseBackupVersion)
		defer api.status.stop(err)
		if err != nil {
			api.metrics.FailedCounter["create"].Inc()
			api.metrics.LastStatus["create"].Set(0)
			apexLog.Errorf("CreateBackup error: %v", err)
			return
		}
		if err := api.updateSizeOfLastBackup(); err != nil {
			apexLog.Errorf("update size: %v", err)
		}
		api.metrics.SuccessfulCounter["create"].Inc()
		api.metrics.LastStatus["create"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusCreated, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "create",
		BackupName: backupName,
	})
}

// httpUploadHandler - upload a backup to remote storage
func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "upload", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "upload", err)
		return
	}
	vars := mux.Vars(r)
	diffFrom := ""
	query := r.URL.Query()
	name := vars["name"]
	tablePattern := ""
	schemaOnly := false
	fullCommand := "upload"

	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
		fullCommand = fmt.Sprintf("%s --diff-from=%s", fullCommand, diffFrom)
	}
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if schema, exist := query["schema"]; exist {
		schemaOnly, _ = strconv.ParseBool(schema[0])
		fullCommand += " --schema"
	}
	fullCommand = fmt.Sprint(fullCommand, " ", name)

	go func() {
		api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["upload"].Set(float64(start.Unix()))
		defer api.metrics.LastDuration["upload"].Set(float64(time.Since(start).Nanoseconds()))
		defer api.metrics.LastFinish["upload"].Set(float64(time.Now().Unix()))
		b := backup.NewBackuper(cfg)
		err := b.Upload(name, tablePattern, diffFrom, schemaOnly)
		api.status.stop(err)
		if err != nil {
			apexLog.Errorf("Upload error: %+v\n", err)
			api.metrics.FailedCounter["upload"].Inc()
			api.metrics.LastStatus["upload"].Set(0)
			return
		}
		if err := api.updateSizeOfLastBackup(); err != nil {
			apexLog.Errorf("update size: %v", err)
		}
		api.metrics.SuccessfulCounter["upload"].Inc()
		api.metrics.LastStatus["upload"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
		BackupFrom string `json:"backup_from,omitempty"`
		Diff       bool   `json:"diff"`
	}{
		Status:     "acknowledged",
		Operation:  "upload",
		BackupName: name,
		BackupFrom: diffFrom,
		Diff:       diffFrom != "",
	})
}

// httpRestoreHandler - restore a backup from local storage
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "restore", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "restore", err)
		return
	}
	vars := mux.Vars(r)
	tablePattern := ""
	schemaOnly := false
	dataOnly := false
	dropTable := false
	doRestoreRBAC := false
	doRestoreConfigs := false
	fullCommand := "restore"

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["data"]; exist {
		dataOnly = true
		fullCommand += " --data"
	}
	if _, exist := query["drop"]; exist {
		dropTable = true
		fullCommand += " --drop"
	}
	if _, exist := query["rm"]; exist {
		dropTable = true
		fullCommand += " --rm"
	}

	if _, exist := query["do_restore_rbac"]; exist {
		doRestoreRBAC = true
		fullCommand += " --do-restore-rbac"
	}

	if _, exist := query["do_restore_configs"]; exist {
		doRestoreConfigs = true
		fullCommand += " --do-restore-configs"
	}

	name := vars["name"]
	fullCommand = fmt.Sprintf(fullCommand, " ", name)

	go func() {
		api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["restore"].Set(float64(start.Unix()))
		defer api.metrics.LastDuration["restore"].Set(float64(time.Since(start).Nanoseconds()))
		defer api.metrics.LastFinish["restore"].Set(float64(time.Now().Unix()))
		err := backup.Restore(cfg, name, tablePattern, schemaOnly, dataOnly, dropTable, doRestoreRBAC, doRestoreConfigs)
		api.status.stop(err)
		if err != nil {
			apexLog.Errorf("Download error: %+v\n", err)
			api.metrics.FailedCounter["restore"].Inc()
			api.metrics.LastStatus["restore"].Set(0)
			return
		}
		api.metrics.SuccessfulCounter["restore"].Inc()
		api.metrics.LastStatus["restore"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "restore",
		BackupName: name,
	})
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "download", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "download", err)
		return
	}
	vars := mux.Vars(r)
	name := vars["name"]
	query := r.URL.Query()
	tablePattern := ""
	schemaOnly := false
	fullCommand := "download"

	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	fullCommand = fmt.Sprintf(fullCommand, " ", name)

	go func() {
		api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["download"].Set(float64(start.Unix()))
		defer api.metrics.LastDuration["download"].Set(float64(time.Since(start).Nanoseconds()))
		defer api.metrics.LastFinish["download"].Set(float64(time.Now().Unix()))
		b := backup.NewBackuper(cfg)
		err := b.Download(name, tablePattern, schemaOnly)
		api.status.stop(err)
		if err != nil {
			apexLog.Errorf("Download error: %+v\n", err)
			api.metrics.FailedCounter["download"].Inc()
			api.metrics.LastStatus["download"].Set(0)
			return
		}
		if err := api.updateSizeOfLastBackup(); err != nil {
			apexLog.Errorf("update size: %v", err)
		}
		api.metrics.SuccessfulCounter["download"].Inc()
		api.metrics.LastStatus["download"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "download",
		BackupName: name,
	})
}

// httpDeleteHandler - delete a backup from local or remote storage
func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "delete", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	vars := mux.Vars(r)
	fullCommand := fmt.Sprintf("delete %s %s", vars["where"], vars["name"])
	api.status.start(fullCommand)

	switch vars["where"] {
	case "local":
		err = backup.RemoveBackupLocal(cfg, vars["name"])
	case "remote":
		err = backup.RemoveBackupRemote(cfg, vars["name"])
	default:
		err = fmt.Errorf("backup location must be 'local' or 'remote'")
	}
	api.status.stop(err)
	if err != nil {
		apexLog.Errorf("delete backup error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	if err := api.updateSizeOfLastBackup(); err != nil {
		apexLog.Errorf("update size: %v", err)
	}
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
		Location   string `json:"location"`
	}{
		Status:     "success",
		Operation:  "delete",
		BackupName: vars["name"],
		Location:   vars["where"],
	})
}

func (api *APIServer) httpBackupStatusHandler(w http.ResponseWriter, _ *http.Request) {
	sendJSONEachRow(w, http.StatusOK, api.status.status())
}

func (api *APIServer) updateSizeOfLastBackup() error {
	if !api.config.API.EnableMetrics {
		return nil
	}
	localBackups, err := backup.GetLocalBackups(api.config)
	if err != nil {
		return err
	}
	if len(localBackups) > 0 {
		api.metrics.LastBackupSizeLocal.Set(float64(localBackups[len(localBackups)-1].DataSize))
	} else {
		api.metrics.LastBackupSizeLocal.Set(0)
	}
	if api.config.General.RemoteStorage == "none" {
		return nil
	}
	remoteBackups, err := backup.GetRemoteBackups(api.config)
	if err != nil {
		return err
	}
	if len(remoteBackups) > 0 {
		api.metrics.LastBackupSizeRemote.Set(float64(remoteBackups[len(remoteBackups)-1].DataSize))
	} else {
		api.metrics.LastBackupSizeRemote.Set(0)
	}
	return nil
}

func registerMetricsHandlers(r *mux.Router, enablemetrics bool, enablepprof bool) {
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		sendJSONEachRow(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{
			Status: "OK",
		})
	})
	if enablemetrics {
		r.Handle("/metrics", promhttp.Handler())
	}
	if enablepprof {
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
		r.Handle("/debug/pprof/block", pprof.Handler("block"))
		r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	}
}

type Metrics struct {
	SuccessfulCounter map[string]prometheus.Counter
	FailedCounter     map[string]prometheus.Counter
	LastStart         map[string]prometheus.Gauge
	LastFinish        map[string]prometheus.Gauge
	LastDuration      map[string]prometheus.Gauge
	LastStatus        map[string]prometheus.Gauge

	LastBackupSizeLocal  prometheus.Gauge
	LastBackupSizeRemote prometheus.Gauge
}

// setupMetrics - resister prometheus metrics
func setupMetrics() Metrics {
	m := Metrics{}
	successfulCounter := map[string]prometheus.Counter{}
	failedCounter := map[string]prometheus.Counter{}
	lastStart := map[string]prometheus.Gauge{}
	lastFinish := map[string]prometheus.Gauge{}
	lastDuration := map[string]prometheus.Gauge{}
	lastStatus := map[string]prometheus.Gauge{}

	for _, command := range []string{"create", "upload", "download", "restore", "create_remote", "restore_remote"} {
		successfulCounter[command] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("successful_%ss", command),
			Help:      fmt.Sprintf("Counter of successful %ss backup", command),
		})
		failedCounter[command] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("failed_%ss", command),
			Help:      fmt.Sprintf("Counter of failed %ss backup", command),
		})
		lastStart[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_start", command),
			Help:      fmt.Sprintf("Last backup %s start timestamp", command),
		})
		lastFinish[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_finish", command),
			Help:      fmt.Sprintf("Last backup %s finish timestamp", command),
		})
		lastDuration[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_duration", command),
			Help:      fmt.Sprintf("Backup %s duration in nanoseconds", command),
		})
		lastStatus[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_status", command),
			Help:      fmt.Sprintf("Last backup %s status: 0=failed, 1=success, 2=unknown", command),
		})
	}

	m.SuccessfulCounter = successfulCounter
	m.FailedCounter = failedCounter
	m.LastStart = lastStart
	m.LastFinish = lastFinish
	m.LastDuration = lastDuration
	m.LastStatus = lastStatus

	m.LastBackupSizeLocal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_size_local",
		Help:      "Last local backup size in bytes",
	})
	m.LastBackupSizeRemote = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_size_remote",
		Help:      "Last remote backup size in bytes",
	})

	prometheus.MustRegister(
		m.SuccessfulCounter["create"],
		m.FailedCounter["create"],
		m.LastStart["create"],
		m.LastFinish["create"],
		m.LastDuration["create"],
		m.LastStatus["create"],

		m.SuccessfulCounter["upload"],
		m.FailedCounter["upload"],
		m.LastStart["upload"],
		m.LastFinish["upload"],
		m.LastDuration["upload"],
		m.LastStatus["upload"],

		m.SuccessfulCounter["download"],
		m.FailedCounter["download"],
		m.LastStart["download"],
		m.LastFinish["download"],
		m.LastDuration["download"],
		m.LastStatus["download"],

		m.SuccessfulCounter["restore"],
		m.FailedCounter["restore"],
		m.LastStart["restore"],
		m.LastFinish["restore"],
		m.LastDuration["restore"],
		m.LastStatus["restore"],

		m.SuccessfulCounter["create_remote"],
		m.FailedCounter["create_remote"],
		m.LastStart["create_remote"],
		m.LastFinish["create_remote"],
		m.LastDuration["create_remote"],
		m.LastStatus["create_remote"],

		m.SuccessfulCounter["restore_remote"],
		m.FailedCounter["restore_remote"],
		m.LastStart["restore_remote"],
		m.LastFinish["restore_remote"],
		m.LastDuration["restore_remote"],
		m.LastStatus["restore_remote"],

		m.LastBackupSizeLocal,
		m.LastBackupSizeRemote,
	)
	m.LastStatus["create"].Set(2) // 0=failed, 1=success, 2=unknown
	m.LastStatus["upload"].Set(2)
	m.LastStatus["download"].Set(2)
	m.LastStatus["restore"].Set(2)
	m.LastStatus["create_remote"].Set(2)
	m.LastStatus["restore_remote"].Set(2)

	return m
}

func (api *APIServer) CreateIntegrationTables() error {
	ch := &clickhouse.ClickHouse{
		Config: &api.config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %w", err)
	}
	defer ch.Close()
	port := strings.Split(api.config.API.ListenAddr, ":")[1]
	auth := ""
	if api.config.API.Username != "" || api.config.API.Password != "" {
		params := url.Values{}
		params.Add("user", api.config.API.Username)
		params.Add("pass", api.config.API.Password)
		auth = fmt.Sprintf("?%s", params.Encode())
	}
	schema := "http"
	if api.config.API.Secure {
		schema = "https"
	}
	query := fmt.Sprintf("CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String) ENGINE=URL('%s://127.0.0.1:%s/backup/actions%s', JSONEachRow)", schema, port, auth)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_actions"}, query, true); err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String, desc String) ENGINE=URL('%s://127.0.0.1:%s/backup/list%s', JSONEachRow)", schema, port, auth)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_list"}, query, true); err != nil {
		return err
	}
	return nil
}
