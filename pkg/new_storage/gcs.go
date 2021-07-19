package new_storage

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client *storage.Client
	Config *config.GCSConfig
	buffer []byte
}

// Connect - connect to GCS
func (gcs *GCS) Connect() error {
	var err error
	var clientOption option.ClientOption
	gcs.buffer = make([]byte, 4*1024*1024)
	ctx := context.Background()

	if gcs.Config.CredentialsJSON != "" {
		clientOption = option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON))
		gcs.client, err = storage.NewClient(ctx, clientOption)
		return err
	}
	if gcs.Config.CredentialsFile != "" {
		clientOption = option.WithCredentialsFile(gcs.Config.CredentialsFile)
		gcs.client, err = storage.NewClient(ctx, clientOption)
		return err
	}
	gcs.client, err = storage.NewClient(ctx)
	return err
}

func (gcs *GCS) Walk(gcsPath string, recursive bool, process func(r RemoteFile) error) error {
	ctx := context.Background()
	delimiter := "/"
	if !recursive {
		delimiter = "/"
	}
	it := gcs.client.Bucket(gcs.Config.Bucket).Objects(ctx, &storage.Query{
		Prefix:    gcsPath,
		Delimiter: delimiter,
	})
	for {
		object, err := it.Next()
		switch err {
		case nil:
			if object.Prefix != "" {
				if err := process(&gcsFile{
					name: strings.TrimPrefix(object.Prefix, path.Join(gcs.Config.Path, "/")),
				}); err != nil {
					return err
				}
				continue
			}
			if err := process(&gcsFile{
				size:         object.Size,
				lastModified: object.Updated,
				name:         strings.TrimPrefix(object.Name, path.Join(gcs.Config.Path, "/")),
			}); err != nil {
				return err
			}
		case iterator.Done:
			return nil
		default:
			return err
		}
	}
}

func (gcs *GCS) Kind() string {
	return "GCS"
}

func (gcs *GCS) GetFileReader(key string) (io.ReadCloser, error) {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (gcs *GCS) GetFileWriter(key string) io.WriteCloser {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key))
	return obj.NewWriter(ctx)
}

func (gcs *GCS) PutFile(key string, r io.ReadCloser) error {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key))
	writer := obj.NewWriter(ctx)
	defer writer.Close()
	_, err := io.CopyBuffer(writer, r, gcs.buffer)
	return err
}

func (gcs *GCS) StatFile(key string) (RemoteFile, error) {
	ctx := context.Background()
	objAttr, err := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key)).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &gcsFile{
		size:         objAttr.Size,
		lastModified: objAttr.Updated,
		name:         objAttr.Name,
	}, nil
}

func (gcs *GCS) DeleteFile(key string) error {
	ctx := context.Background()
	object := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key))
	return object.Delete(ctx)
}

type gcsFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *gcsFile) Size() int64 {
	return f.size
}

func (f *gcsFile) Name() string {
	return f.name
}

func (f *gcsFile) LastModified() time.Time {
	return f.lastModified
}
