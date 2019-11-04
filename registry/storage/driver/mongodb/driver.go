package mongodb

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const driverName = "mongodb"
const fs = "registry"
const separator = "/"

func init() {
	factory.Register(driverName, &mongodbDriverFactory{})
}

// mongodbDriverFactory implements the factory.StorageDriverFactory interface.
type mongodbDriverFactory struct{}

type driver struct {
	client *mongo.Client
	db     *mongo.Database
	config *connectionConfig
}

// baseEmbed allows us to hide the Base embed.
type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local map.
// Intended solely for example and testing purposes.
type Driver struct {
	baseEmbed // embedded, hidden base driver.
}

type gridFsEntry struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	Filename   string
	UploadDate time.Time `bson:"uploadDate"`
	Length     int64
}

type databaseConfigEntry struct {
	ID          string `bson:"_id,omitempty"`
	Partitioned bool
}

type serverStatusEntry struct {
	Process string
}

type connectionConfig struct {
	timeout      *time.Duration
	readConcern  *readconcern.ReadConcern
	writeConcern *writeconcern.WriteConcern
	readPref     *readpref.ReadPref
}

var _ storagedriver.StorageDriver = &Driver{}

func (factory *mongodbDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters constructs a new Driver with a given parameters map.
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	url, ok := parameters["url"]
	if !ok || fmt.Sprint(url) == "" {
		return nil, fmt.Errorf("no 'url' parameter provided")
	}
	databaseName, ok := parameters["dbname"]
	if !ok || fmt.Sprint(databaseName) == "" {
		databaseName = "docker"
	}
	return New(fmt.Sprint(url), fmt.Sprint(databaseName), getConnectionConfig(parameters))
}

func getConnectionConfig(parameters map[string]interface{}) *connectionConfig {
	connectionConfig := &connectionConfig{}
	defaultTimeout := 120 * time.Second
	connectionConfig.timeout = &defaultTimeout
	timeoutAsString, ok := parameters["timeout"]
	if ok {
		timeoutAsInt, err := strconv.Atoi(fmt.Sprint(timeoutAsString))
		if err == nil {
			timeoutInSecond := time.Duration(timeoutAsInt) * time.Second
			connectionConfig.timeout = &timeoutInSecond
		}
	}

	connectionConfig.writeConcern = getWriteConcern(parameters)
	connectionConfig.readConcern = getReadConcern(parameters)
	connectionConfig.readPref = getReadPref(parameters)
	return connectionConfig
}

func getWriteConcern(parameters map[string]interface{}) *writeconcern.WriteConcern {
	var writeOptions []writeconcern.Option
	wOption := writeconcern.W(1)
	wAsString, ok := parameters["writeconcern_w"]
	if ok {
		wAsInt, err := strconv.Atoi(fmt.Sprint(wAsString))
		if err == nil {
			wOption = writeconcern.W(wAsInt)
		}
	}
	writeOptions = append(writeOptions, wOption)
	jAsString, ok := parameters["writeconcern_j"]
	if ok {
		jAsBool, err := strconv.ParseBool(fmt.Sprint(jAsString))
		if err == nil {
			writeOptions = append(writeOptions, writeconcern.J(jAsBool))
		}
	}
	wTimeoutAsString, ok := parameters["writeconcern_wtimeout"]
	if ok {
		wTimeoutAsInt, err := strconv.Atoi(fmt.Sprint(wTimeoutAsString))
		if err == nil {
			timeoutInSecond := time.Duration(wTimeoutAsInt) * time.Second
			writeOptions = append(writeOptions, writeconcern.WTimeout(timeoutInSecond))
		}
	}
	return writeconcern.New(writeOptions...)
}

func getReadConcern(parameters map[string]interface{}) *readconcern.ReadConcern {
	var readOptions []readconcern.Option
	levelAsString, ok := parameters["readconcern_level"]
	if ok {
		readOptions = append(readOptions, readconcern.Level(fmt.Sprint(levelAsString)))
	}
	return readconcern.New(readOptions...)
}

func getReadPref(parameters map[string]interface{}) *readpref.ReadPref {
	var readOptions []readpref.Option
	stalenessAsString, ok := parameters["readpref_maxstaleness"]
	if ok {
		stalenessAsInt, err := strconv.Atoi(fmt.Sprint(stalenessAsString))
		if err == nil {
			stalenessInSecond := time.Duration(stalenessAsInt) * time.Second
			readOptions = append(readOptions, readpref.WithMaxStaleness(stalenessInSecond))
		}
	}
	mode := readpref.PrimaryMode
	modeAsString, ok := parameters["readpref_mode"]
	if ok {
		customMode, err := readpref.ModeFromString(fmt.Sprint(modeAsString))
		if err == nil {
			mode = customMode
		}
	}
	readPref, err := readpref.New(mode, readOptions...)
	if err != nil {
		logrus.Warnf("unable to parse 'readpref' config params, using default 'Primary' configuration. Error: %v", err)
		return readpref.Primary()
	}
	return readPref
}

// New constructs a new Driver.
func New(url, databaseName string, config *connectionConfig) (*Driver, error) {
	logrus.Infof("Connecting to the mongodb with the following configuration: {timeout: %v, writeConcern: %+v, readConcern: %+v, readPref: %+v}", config.timeout, config.writeConcern, config.readConcern, config.readPref)
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	databaseOptions := options.Database().SetWriteConcern(config.writeConcern).SetReadConcern(config.readConcern).SetReadPreference(config.readPref)
	d := &driver{
		client: client,
		db:     client.Database(databaseName, databaseOptions),
		config: config,
	}
	isShard, err := isShardEnvironment(ctx, d)
	if err != nil {
		return nil, err
	}
	if isShard {
		err := initShard(ctx, d)
		if err != nil {
			return nil, err
		}
	}
	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

func isShardEnvironment(ctx context.Context, d *driver) (bool, error) {
	singleResult := d.client.Database("admin").RunCommand(ctx, bson.M{"serverStatus": 1})
	if singleResult.Err() != nil {
		return false, singleResult.Err()
	}
	var serverStatus serverStatusEntry
	err := singleResult.Decode(&serverStatus)
	if err != nil {
		return false, err
	}
	return serverStatus.Process == "mongos", nil
}

func initShard(ctx context.Context, d *driver) error {
	err := createGridFSDatabase(ctx, d)
	if err != nil {
		return err
	}
	err = enableDatabaseSharding(ctx, d)
	if err != nil {
		return err
	}
	err = createShardKeys(ctx, d)
	if err != nil {
		return err
	}
	return nil
}

func createGridFSDatabase(ctx context.Context, d *driver) error {
	count, err := d.gridFSFilesCollection().CountDocuments(ctx, bson.M{})
	if err != nil {
		return err
	}
	if count == 0 {
		tempContent := make([]byte, 2)
		tempFilename := "temp_file"
		err := d.PutContent(ctx, tempFilename, tempContent)
		if err != nil {
			return err
		}
		e := d.Delete(ctx, tempFilename)
		if e != nil {
			return err
		}
	}
	return nil
}

func enableDatabaseSharding(ctx context.Context, d *driver) error {
	var configEntries databaseConfigEntry
	singleResult := d.client.Database("config").Collection("databases").FindOne(ctx, bson.M{"_id": d.db.Name()})
	if singleResult.Err() != nil {
		return singleResult.Err()
	}
	err := singleResult.Decode(&configEntries)
	if err != nil {
		return err
	}
	if !configEntries.Partitioned {
		result := d.client.Database("admin").RunCommand(ctx, bson.M{"enableSharding": d.db.Name()})
		if result.Err() != nil {
			return result.Err()
		}
	}
	return nil
}

func createShardKeys(ctx context.Context, d *driver) error {
	chunksCollection := d.db.Name() + "." + fs + ".chunks"
	err := createShardKey(ctx, d, chunksCollection, bson.D{
		{Key: "shardCollection", Value: chunksCollection},
		{Key: "key", Value: bson.D{{Key: "files_id", Value: 1}, {Key: "n", Value: 1}}},
	})
	if err != nil {
		return err
	}

	filesCollection := d.db.Name() + "." + fs + ".files"
	err = createShardKey(ctx, d, filesCollection, bson.D{
		{Key: "shardCollection", Value: filesCollection},
		{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
	})
	if err != nil {
		return err
	}
	return nil
}

func createShardKey(ctx context.Context, d *driver, collectionName string, shardKeyCmd interface{}) error {
	cursor, err := d.client.Database("config").Collection("collections").Find(ctx, bson.M{"_id": collectionName})
	if err != nil {
		return err
	}
	defer closeCursor(ctx, cursor)
	if !cursor.Next(ctx) {
		singleResult := d.client.Database("admin").RunCommand(ctx, shardKeyCmd)
		if singleResult.Err() != nil {
			return singleResult.Err()
		}
	}
	return nil
}

// Implement the storagedriver.StorageDriver interface.

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}
	result, err := ioutil.ReadAll(rc)
	closeErr := rc.Close()
	if closeErr != nil {
		return nil, closeErr
	}
	return result, err
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, p string, contents []byte) error {
	deleteErr := d.deleteByFilename(ctx, p)
	if deleteErr != nil && deleteErr != mongo.ErrNoDocuments {
		return deleteErr
	}
	file, err := d.gridFS().OpenUploadStream(p)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(contents)
	if writeErr != nil {
		return writeErr
	}
	closeErr := file.Close()
	if closeErr != nil {
		return closeErr
	}
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	file, err := d.gridFS().OpenDownloadStreamByName(path)
	if err != nil {
		if err == gridfs.ErrFileNotFound {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}
	if offset > 0 {
		skipArray := make([]byte, offset)
		_, err = file.Read(skipArray)
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	file, err := d.gridFS().OpenUploadStream(path)
	if err != nil {
		return nil, err
	}

	var existingFileSize int64
	existingFile, err := d.gridFS().OpenDownloadStreamByName(path)
	if err == nil { //file exists
		if append {
			written, copyErr := io.Copy(file, existingFile)
			if copyErr != nil {
				return nil, copyErr
			}
			existingFileSize = written
			err := existingFile.Close()
			if err != nil {
				return nil, err
			}
		}
		err := d.deleteByFilename(ctx, path)
		if err != nil {
			return nil, err
		}
	} else {
		if append {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
	}
	return &writer{
		driver: d,
		file:   file,
		size:   existingFileSize,
	}, nil
}

// Stat returns info about the provided path.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	file, err := d.getFileByName(ctx, path)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return dirStat(ctx, d, path)
		}
		return nil, err
	}
	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   false,
		ModTime: file.UploadDate,
		Size:    file.Length,
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func dirStat(ctx context.Context, d *driver, path string) (storagedriver.FileInfo, error) {
	var files []gridFsEntry
	cursor, err := d.gridFS().Find(bson.M{"filename": bson.M{"$regex": path + ".*"}})
	if err != nil {
		return nil, err
	}
	defer closeCursor(ctx, cursor)
	files, err = getAll(ctx, cursor)
	if err != nil {
		return nil, err
	}
	if len(files) > 0 {
		return storagedriver.FileInfoInternal{FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			IsDir:   true,
			ModTime: getTimestamp(files[0].ID),
		}}, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

func getAll(ctx context.Context, cursor *mongo.Cursor) ([]gridFsEntry, error) {
	var files []gridFsEntry
	for {
		condition := cursor.Next(ctx)
		if !condition {
			return files, nil
		}
		entry := gridFsEntry{}
		err := cursor.Decode(&entry)
		if err != nil {
			return nil, err
		}
		files = append(files, entry)
	}
}

func closeCursor(ctx context.Context, cursor *mongo.Cursor) {
	err := cursor.Close(ctx)
	if err != nil {
		logrus.Warnf("Error while closing cursor")
	}
}

func getTimestamp(id primitive.ObjectID) time.Time {
	unixSecs := binary.BigEndian.Uint32(id[0:4])
	return time.Unix(int64(unixSecs), 0).UTC()
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	var files []gridFsEntry
	if !strings.HasSuffix(path, separator) {
		path += separator
	}
	cursor, err := d.gridFS().Find(bson.M{"filename": bson.M{"$regex": path + ".*"}})
	if err != nil {
		return nil, err
	}
	defer closeCursor(ctx, cursor)
	files, err = getAll(ctx, cursor)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for i := 0; i < len(files); i++ {
		filename := files[i].Filename
		descendant := strings.TrimPrefix(filename, path)
		if descendant != filename {
			set[path+strings.SplitN(descendant, separator, 2)[0]] = true
		}
	}
	if path != separator && len(set) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: strings.TrimSuffix(path, separator)}
	}

	result := make([]string, len(set))
	index := 0
	for key := range set {
		result[index] = key
		index++
	}
	return result, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	destFile, err := d.gridFS().OpenUploadStream(destPath)
	if err != nil {
		return err
	}
	sourceFile, err := d.Reader(ctx, sourcePath, 0)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	_, copyErr := io.Copy(destFile, sourceFile)
	if copyErr != nil {
		return copyErr
	}
	removeErr := d.deleteByFilename(ctx, sourcePath)
	if removeErr != nil {
		return removeErr
	}
	return destFile.Close()
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	var files []gridFsEntry
	cursor, err := d.gridFS().Find(bson.M{"$or": []bson.M{
		{"filename": bson.M{"$regex": path + "/.*"}},
		{"filename": bson.M{"$eq": path}},
	}})
	if err != nil {
		return err
	}
	defer closeCursor(ctx, cursor)
	files, err = getAll(ctx, cursor)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}
	for _, file := range files {
		err := d.gridFS().Delete(file.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) gridFS() *gridfs.Bucket {
	bucket, _ := gridfs.NewBucket(d.db, options.GridFSBucket().SetName(fs).SetWriteConcern(d.config.writeConcern).SetReadPreference(d.config.readPref).SetReadConcern(d.config.readConcern))
	return bucket
}

func (d *driver) gridFSFilesCollection() *mongo.Collection {
	return d.db.Collection(fs + ".files")
}

func (d *driver) deleteByFilename(ctx context.Context, path string) error {
	file, err := d.getFileByName(ctx, path)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	}
	err = d.gridFS().Delete(file.ID)
	if err != nil {
		return err
	}
	return nil
}

func (d *driver) getFileByName(ctx context.Context, path string) (*gridFsEntry, error) {
	singleResult := d.gridFSFilesCollection().FindOne(ctx, bson.M{"filename": path})
	if singleResult.Err() != nil {
		return nil, singleResult.Err()
	}
	var file gridFsEntry
	err := singleResult.Decode(&file)
	if err != nil {
		return nil, err
	}
	return &file, nil
}

func (d *driver) decorateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return d.config.decorateContext(ctx)
}

func (c *connectionConfig) decorateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout != nil {
		ctx, cancel := context.WithTimeout(ctx, *c.timeout)
		return ctx, cancel
	}
	return ctx, func() {}
}

//**********************************************************************************************************************
// FileWriter implementation
//**********************************************************************************************************************
type writer struct {
	driver    *driver
	file      *gridfs.UploadStream
	size      int64
	closed    bool
	committed bool
	cancelled bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	written, err := w.file.Write(p)
	if err == nil {
		w.size += int64(written)
	}
	return written, err
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.file.Close()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	return w.file.Abort()
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	w.committed = true
	return nil
}
