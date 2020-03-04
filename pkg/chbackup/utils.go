package chbackup

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/mholt/archiver"
)

type Backup struct {
	Name string
	Size int64
	Date time.Time
}

func cleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func isClickhouseShadow(path string) bool {
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

func moveShadow(shadowPath, backupPath, backupName string, version int) error {
	switch {
	case version < 19010001: // 19.10
		break
	case len(backupName) == 0:
		data, err := ioutil.ReadFile(fmt.Sprintf("%s%c%s", shadowPath, os.PathSeparator, "increment.txt"))
		if err != nil {
			return err
		}
		data = data[:len(data)-1]
		idx, err := strconv.Atoi(string(data))
		if err != nil {
			return err
		}
		shadowPath = fmt.Sprintf("%s%c%d", shadowPath, os.PathSeparator, idx)
	default:
		shadowPath = fmt.Sprintf("%s%c%s", shadowPath, os.PathSeparator, base58.Encode([]byte(backupName)))
	}
	if err := filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		switch {
		case version < 19010001:
			relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
			pathParts := strings.SplitN(relativePath, "/", 3)
			if len(pathParts) != 3 {
				return nil
			}
			dstFilePath := filepath.Join(backupPath, pathParts[2])
			if info.IsDir() {
				return os.MkdirAll(dstFilePath, os.ModePerm)
			}
			if !info.Mode().IsRegular() {
				log.Printf("'%s' is not a regular file, skipping", filePath)
				return nil
			}
			return os.Rename(filePath, dstFilePath)
		default:
			relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
			pathParts := strings.SplitN(relativePath, "/", 2)
			if len(pathParts) != 2 {
				return nil
			}
			dstFilePath := filepath.Join(backupPath, pathParts[1])
			if info.IsDir() {
				return os.MkdirAll(dstFilePath, os.ModePerm)
			}
			if !info.Mode().IsRegular() {
				log.Printf("'%s' is not a regular file, skipping", filePath)
				return nil
			}
			return os.Rename(filePath, dstFilePath)
		}
	}); err != nil {
		return err
	}
	err := cleanDir(shadowPath)
	if version >= 19010001 {
		err = os.RemoveAll(shadowPath)
	}
	return err
}

func copyFile(srcFile string, dstFile string) error {
	if err := os.MkdirAll(path.Dir(dstFile), os.ModePerm); err != nil {
		return err
	}
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].Date.After(backups[j].Date)
		})
		return backups[keep:]
	}
	return []Backup{}
}

func getArchiveWriter(format string, level int) (archiver.Writer, error) {
	switch format {
	case "tar":
		return &archiver.Tar{}, nil
	case "lz4":
		return &archiver.TarLz4{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "bzip2":
		return &archiver.TarBz2{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "gzip":
		return &archiver.TarGz{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "sz":
		return &archiver.TarSz{Tar: archiver.NewTar()}, nil
	case "xz":
		return &archiver.TarXz{Tar: archiver.NewTar()}, nil
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'lz4', 'bzip2', 'gzip', 'sz', 'xz'")
}

func getExtension(format string) string {
	switch format {
	case "tar":
		return "tar"
	case "lz4":
		return "tar.lz4"
	case "bzip2":
		return "tar.bz2"
	case "gzip":
		return "tar.gz"
	case "sz":
		return "tar.sz"
	case "xz":
		return "tar.xz"
	}
	return ""
}

func getArchiveReader(format string) (archiver.Reader, error) {
	switch format {
	case "tar":
		return archiver.NewTar(), nil
	case "lz4":
		return archiver.NewTarLz4(), nil
	case "bzip2":
		return archiver.NewTarBz2(), nil
	case "gzip":
		return archiver.NewTarGz(), nil
	case "sz":
		return archiver.NewTarSz(), nil
	case "xz":
		return archiver.NewTarXz(), nil
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'")
}

// FormatBytes - Convert bytes to human readable string
func FormatBytes(i int64) (result string) {
	const (
		KiB = 1024
		MiB = 1048576
		GiB = 1073741824
		TiB = 1099511627776
	)
	switch {
	case i >= TiB:
		result = fmt.Sprintf("%.02f TiB", float64(i)/TiB)
	case i >= GiB:
		result = fmt.Sprintf("%.02f GiB", float64(i)/GiB)
	case i >= MiB:
		result = fmt.Sprintf("%.02f MiB", float64(i)/MiB)
	case i >= KiB:
		result = fmt.Sprintf("%.02f KiB", float64(i)/KiB)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	return
}

func TablePathEncode(str string) string {
	return strings.ReplaceAll(url.PathEscape(str), ".", "%2E")
}

func parseTime(text string) (t time.Time, err error) {
	timeFormats := []string{
		"Mon, 02 Jan 2006 15:04:05 GMT",
		time.RFC850,
		time.ANSIC,
		time.RFC3339,
	}

	for _, layout := range timeFormats {
		t, err = time.Parse(layout, text)
		if err == nil {
			return
		}
	}
	return
}
