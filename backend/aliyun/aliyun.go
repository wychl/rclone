// Package aliyun oss storage
package aliyun

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config/configmap"
	"github.com/ncw/rclone/fs/config/configstruct"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/fs/walk"
	"github.com/ncw/rclone/lib/rest"
	"github.com/ncw/swift"
	"github.com/pkg/errors"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "aliyun",
		Description: "Alibaba Cloud OSS.",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name: "access_key_id",
				Help: "Alibaba Cloud OSS Access Key ID.\nLeave blank for anonymous access or runtime credentials.",
			},
			{
				Name: "access_key_secret",
				Help: "Alibaba Cloud OSS Secret Access Key (password)\nLeave blank for anonymous access or runtime credentials.",
			},
			{
				Name: "endpoint",
				Help: "Endpoint for Alibaba Cloud OSS API",
			},

			{
				Name: "acl",
				Help: "Canned ACL used when creating buckets and/or storing objects in oss.",
				Examples: []fs.OptionExample{{
					Value: "private",
					Help:  "Owner gets FULL_CONTROL. No one else has access rights (default).",
				},
					{
						Value: "public-read",
						Help:  "Owner gets FULL_CONTROL. The AllUsers group gets READ access.",
					},
					{
						Value: "public-read-write",
						Help:  "Owner gets FULL_CONTROL. The AllUsers group gets READ and WRITE access.\nGranting this on a bucket is generally not recommended.",
					}},
			},

			{
				Name:     "chunk_size",
				Help:     "Chunk size to use for uploading",
				Default:  fs.SizeSuffix(minUploadPartSize),
				Advanced: true,
			},
			{
				Name:     "upload_concurrency",
				Help:     "Concurrency for multipart uploads.",
				Default:  2,
				Advanced: true,
			},
			{
				Name: "server_side_encryption",
				Help: "The server-side encryption algorithm used when storing this object in oss.",
				Examples: []fs.OptionExample{{
					Value: "AES256",
					Help:  "AES256",
				}},
			},

			{
				Name: "storage_class",
				Help: "The storage class to use when storing objects in oss.",
				Examples: []fs.OptionExample{{
					Value: "",
					Help:  "Default",
				}, {
					Value: "STANDARD",
					Help:  "Standard storage class",
				}, {
					Value: "REDUCED_REDUNDANCY",
					Help:  "Reduced redundancy storage class",
				}, {
					Value: "STANDARD_IA",
					Help:  "Standard Infrequent Access storage class",
				}, {
					Value: "ONEZONE_IA",
					Help:  "One Zone Infrequent Access storage class",
				}},
			},
		}})
}

// Constants
const (
	metaMtime               = "Mtime"                       // the meta key to store mtime in - eg X-Amz-Meta-Mtime
	metaMD5Hash             = "Md5chksum"                   // the meta key to store md5hash in
	listChunkSize           = 1000                          // number of items to read at once
	maxRetries              = 10                            // number of retries to make of operations
	maxSizeForCopy          = 5 * 1024 * 1024 * 1024        // The maximum size of object we can COPY
	maxFileSize             = 5 * 1024 * 1024 * 1024 * 1024 // largest possible upload file size
	minUploadPartSize int64 = 1024 * 1024 * 5
)

// Options defines the configuration for this backend
type Options struct {
	AccessKeyID          string        `config:"access_key_id"`
	AccessKeySecret      string        `config:"access_key_secret"`
	ACL                  string        `config:"acl"`
	Region               string        `config:"region"`
	Endpoint             string        `config:"endpoint"`
	ChunkSize            fs.SizeSuffix `config:"chunk_size"`
	DisableChecksum      bool          `config:"disable_checksum"`
	SessionToken         string        `config:"session_token"`
	UploadConcurrency    int           `config:"upload_concurrency"`
	ForcePathStyle       bool          `config:"force_path_style"`
	ServerSideEncryption string        `config:"server_side_encryption"`
	StorageClass         string        `config:"storage_class"`
}

// Fs represents a remote oss server
type Fs struct {
	name          string       // the name of the remote
	root          string       // root of the bucket - ignore all objects above this
	opt           Options      // parsed options
	features      *fs.Features // optional features
	c             *oss.Client  // the connection to the aliyun server
	bucket        *oss.Bucket
	bucketName    string     // the bucket we are working on
	bucketOKMu    sync.Mutex // mutex to protect bucket OK
	bucketOK      bool       // true if we have created the bucket
	bucketDeleted bool       // true if we have deleted the bucket
}

// Object describes a oss object
type Object struct {
	// Will definitely have everything but meta which may be nil
	//
	// List will read everything but meta & mimeType - to fill
	// that in you need to call readMetaData
	fs           *Fs                // what this object is part of
	remote       string             // The remote path
	etag         string             // md5sum of the object
	bytes        int64              // size of the object
	lastModified time.Time          // Last modified
	meta         map[string]*string // The object metadata if known - may be nil
	mimeType     string             // MimeType of object - may be ""
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	if f.root == "" {
		return f.bucketName
	}
	return f.bucketName + "/" + f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.root == "" {
		return fmt.Sprintf("oss bucket %s", f.bucketName)
	}
	return fmt.Sprintf("oss bucket %s path %s", f.bucketName, f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Pattern to match a oss path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parseParse parses a oss 'url'
func aliyunParsePath(path string) (bucket, directory string, err error) {
	parts := matcher.FindStringSubmatch(path)
	if parts == nil {
		err = errors.Errorf("couldn't parse bucket out of aliyun cos path %q", path)
	} else {
		bucket, directory = parts[1], parts[2]
		directory = strings.Trim(directory, "/")
	}
	return
}

// ossConnection makes a connection to aliyun oss
func ossConnection(opt *Options) (*oss.Client, error) {
	c, err := oss.New(opt.Endpoint, opt.AccessKeyID, opt.AccessKeySecret)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func setAcl(client *oss.Client, acl, bucket string) error {
	var bucketACL oss.ACLType

	switch oss.ACLType(acl) {
	case oss.ACLPrivate:
		bucketACL = oss.ACLPrivate
	case oss.ACLPublicRead:
		bucketACL = oss.ACLPublicRead
	case oss.ACLPublicReadWrite:
		bucketACL = oss.ACLPublicReadWrite
	default:
		return fmt.Errorf("upsupported acl ：%s", acl)
	}

	err := client.SetBucketACL(bucket, bucketACL)
	if err != nil {
		return err
	}

	return nil
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.ChunkSize < fs.SizeSuffix(minUploadPartSize) {
		return nil, errors.Errorf("aliyun oss chunk size (%v) must be >= %v", opt.ChunkSize, fs.SizeSuffix(minUploadPartSize))
	}
	bucketName, directory, err := aliyunParsePath(root)
	if err != nil {
		return nil, err
	}

	c, err := ossConnection(opt)
	if err != nil {
		return nil, err
	}

	bucket, err := c.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:       name,
		root:       directory,
		opt:        *opt,
		c:          c,
		bucket:     bucket,
		bucketName: bucketName,
	}

	f.features = (&fs.Features{
		ReadMimeType:  true,
		WriteMimeType: true,
		BucketBased:   true,
	}).Fill(f)
	if f.root != "" {
		f.root += "/"
		// Check to see if the object exists
		_, err := bucket.GetObjectMeta(directory)
		if err == nil {
			f.root = path.Dir(directory)
			if f.root == "." {
				f.root = ""
			} else {
				f.root += "/"
			}
			// return an error with an fs which points to the parent
			return f, fs.ErrorIsFile
		}
	}
	// f.listMultipartUploads()
	return f, nil
}

// Return an Object from a path
//
//If it can't be found it returns the error ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(remote string, info *oss.ObjectProperties) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	if info != nil {
		// Set info but not meta
		if (info.LastModified == time.Time{}) {
			fs.Logf(o, "Failed to read last modified")
			o.lastModified = time.Now()
		} else {
			o.lastModified = info.LastModified
		}

		o.etag = info.ETag
		o.bytes = info.Size
	} else {
		err := o.readMetaData() // reads info and meta, returning an error
		if err != nil {
			return nil, err
		}

	}

	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	return f.newObjectWithInfo(remote, nil)
}

// listFn is called from list to handle an object.
type listFn func(remote string, object *oss.ObjectProperties, isDirectory bool) error

// list the objects into the function supplied
//
// dir is the starting directory, "" for root
//
// Set recurse to read sub directories
func (f *Fs) list(dir string, recurse bool, fn listFn) error {
	root := f.root
	if dir != "" {
		root += dir + "/"
	}

	maxKeys := listChunkSize
	delimiter := ""
	if !recurse {
		delimiter = "/"
	}
	var marker string
	for {
		listObjects, err := f.bucket.ListObjects(oss.Prefix(root), oss.Marker(marker), oss.MaxKeys(maxKeys), oss.Delimiter(delimiter))
		if err != nil {
			return err
		}

		rootLength := len(f.root)
		if !recurse {
			for _, commonPrefix := range listObjects.CommonPrefixes {
				if commonPrefix == "" {
					fs.Logf(f, "Nil common prefix received")
					continue
				}
				remote := commonPrefix
				if !strings.HasPrefix(remote, f.root) {
					fs.Logf(f, "Odd name received %q", remote)
					continue
				}
				remote = remote[rootLength:]
				if strings.HasSuffix(remote, "/") {
					remote = remote[:len(remote)-1]
				}
				err = fn(remote, &oss.ObjectProperties{Key: remote}, true)
				if err != nil {
					return err
				}
			}
		}

		for _, object := range listObjects.Objects {
			key := object.Key

			if !strings.HasPrefix(key, f.root) {
				fs.Logf(f, "Odd name received %q", key)
				continue
			}
			remote := key[rootLength:]
			// is this a directory marker?
			if (strings.HasSuffix(remote, "/") || remote == "") && object.Size == 0 {
				if recurse && remote != "" {
					// add a directory in if --fast-list since will have no prefixes
					remote = remote[:len(remote)-1]
					err = fn(remote, &oss.ObjectProperties{Key: remote}, true)
					if err != nil {
						return err
					}
				}
				continue // skip directory marker
			}

			err = fn(remote, &object, false)
			if err != nil {
				return err
			}
		}

		if listObjects.IsTruncated {
			break
		}

		// Use NextMarker if set, otherwise use last Key
		if listObjects.NextMarker == "" {
			if len(listObjects.Objects) == 0 {
				return nil
			}
			marker = listObjects.Objects[len(listObjects.Objects)-1].Key
		} else {
			marker = listObjects.NextMarker
		}
	}
	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(remote string, object *oss.ObjectProperties, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		size := int64(0)
		if object.Size != 0 {
			size = object.Size
		}
		d := fs.NewDir(remote, time.Time{}).SetSize(size)
		return d, nil
	}
	o, err := f.newObjectWithInfo(remote, object)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// mark the bucket as being OK
func (f *Fs) markBucketOK() {
	if f.bucketName != "" {
		f.bucketOKMu.Lock()
		f.bucketOK = true
		f.bucketDeleted = false
		f.bucketOKMu.Unlock()
	}
}

// listDir lists files and directories to out
func (f *Fs) listDir(dir string) (entries fs.DirEntries, err error) {
	// List the objects and directories
	err = f.list(dir, false, func(remote string, object *oss.ObjectProperties, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// bucket must be present if listing succeeded
	f.markBucketOK()
	return entries, nil
}

// listBuckets lists the buckets to out
func (f *Fs) listBuckets(dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		return nil, fs.ErrorListBucketRequired
	}

	resp, err := f.c.ListBuckets()
	if err != nil {
		return nil, err
	}

	for _, bucket := range resp.Buckets {
		d := fs.NewDir(bucket.Name, bucket.CreationDate)
		entries = append(entries, d)
	}
	return entries, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	if f.bucketName == "" {
		return f.listBuckets(dir)
	}
	return f.listDir(dir)
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	if f.bucketName == "" {
		return fs.ErrorListBucketRequired
	}
	list := walk.NewListRHelper(callback)
	err = f.list(dir, true, func(remote string, object *oss.ObjectProperties, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		return list.Add(entry)
	})
	if err != nil {
		return err
	}
	// bucket must be present if listing succeeded
	f.markBucketOK()
	return list.Flush()
}

// Put the Object into the bucket
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	fs := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return fs, fs.Update(in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(in, src, options...)
}

// Check if the bucket exists
//
// NB this can return incorrect results if called immediately after bucket deletion
func (f *Fs) dirExists() (bool, error) {
	return f.c.IsBucketExist(f.bucketName)
}

// Mkdir creates the bucket if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.bucketOK {
		return nil
	}
	if !f.bucketDeleted {
		exists, err := f.dirExists()
		if err == nil {
			f.bucketOK = exists
		}
		if err != nil || exists {
			return err
		}
	}

	err := f.c.CreateBucket(f.bucketName, oss.ACL(oss.ACLType(f.opt.ACL)))
	if err != nil {
		return err
	}

	f.bucketOK = true
	f.bucketDeleted = false

	return err
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(dir string) error {
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.root != "" || dir != "" {
		return nil
	}

	err := f.c.DeleteBucket(f.bucketName)
	if err != nil {
		return err
	}

	f.bucketOK = false
	f.bucketDeleted = true

	return nil
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// pathEscape escapes s as for a URL path.  It uses rest.URLPathEscape
// but also escapes '+' for oss and Digital Ocean spaces compatibility
func pathEscape(s string) string {
	return strings.Replace(rest.URLPathEscape(s), "+", "%2B", -1)
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	err := f.Mkdir("")
	if err != nil {
		return nil, err
	}
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	srcFs := srcObj.fs
	key := f.root + remote
	source := pathEscape(srcFs.bucketName + "/" + srcFs.root + srcObj.remote)

	_, err = f.bucket.CopyObject(source, key)
	if err != nil {
		return nil, err
	}
	return f.NewObject(remote)
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

var matchMd5 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	hash := strings.Trim(strings.ToLower(o.etag), `"`)
	// Check the etag is a valid md5sum
	if !matchMd5.MatchString(hash) {
		err := o.readMetaData()
		if err != nil {
			return "", err
		}

		if md5sum, ok := o.meta[metaMD5Hash]; ok {
			md5sumBytes, err := base64.StdEncoding.DecodeString(*md5sum)
			if err != nil {
				return "", err
			}
			hash = hex.EncodeToString(md5sumBytes)
		} else {
			hash = ""
		}
	}
	return hash, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.bytes
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData() (err error) {
	if o.meta != nil {
		return nil
	}
	key := o.fs.root + o.remote

	meta, err := o.fs.bucket.GetObjectDetailedMeta(key)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil
		}
		return err
	}

	length, err := strconv.ParseInt(meta.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return err
	}
	o.etag = meta.Get(oss.HTTPHeaderEtag)
	o.mimeType = meta.Get(oss.HTTPHeaderContentType)
	o.bytes = length

	kv := make(map[string]*string)
	for key, value := range meta {
		kv[key] = &value[0]
	}
	o.meta = kv
	return nil
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	if fs.Config.UseServerModTime {
		return o.lastModified
	}
	err := o.readMetaData()
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	// read mtime out of metadata if available
	d, ok := o.meta[metaMtime]
	if !ok || d == nil {
		// fs.Debugf(o, "No metadata")
		return o.lastModified
	}
	modTime, err := swift.FloatStringToTime(*d)
	if err != nil {
		fs.Logf(o, "Failed to read mtime from object: %v", err)
		return o.lastModified
	}
	return modTime
}

// Turns a number of ns into a floating point string in seconds
//
// Trims trailing zeros and guaranteed to be perfectly accurate
func nsToFloatString(ns int64) string {
	if ns < 0 {
		return "-" + nsToFloatString(-ns)
	}
	result := fmt.Sprintf("%010d", ns)
	split := len(result) - 9
	result, decimals := result[:split], result[split:]
	decimals = strings.TrimRight(decimals, "0")
	if decimals != "" {
		result += "."
		result += decimals
	}
	return result
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) error {
	err := o.readMetaData()
	if err != nil {
		return err
	}

	timeStr := nsToFloatString(modTime.UnixNano())
	o.meta[metaMtime] = &timeStr

	if o.bytes >= maxSizeForCopy {
		fs.Debugf(o, "SetModTime is unsupported for objects bigger than %v bytes", fs.SizeSuffix(maxSizeForCopy))
		return nil
	}

	// Guess the content type
	mimeType := fs.MimeType(o)

	// Copy the object to itself to update the metadata
	key := o.fs.root + o.remote
	sourceKey := o.remote
	directive := oss.MetaReplace // replace metadata with that passed in

	_, err = o.fs.bucket.CopyObject(sourceKey,
		key,
		oss.ContentType(mimeType),
		oss.Meta(metaMtime, timeStr),
		oss.MetadataDirective(directive),
		oss.ContentType(mimeType))

	return err
}

// Storable raturns a boolean indicating if this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	key := o.fs.root + o.remote

	var getObjectOptions []oss.Option
	for _, option := range options {
		switch o := option.(type) {
		case *fs.RangeOption:
			getObjectOptions = append(getObjectOptions, oss.Range(o.Start, o.End))
		case *fs.SeekOption:
			getObjectOptions = append(getObjectOptions, oss.Range(o.Offset, 0))
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	resp, err := o.fs.bucket.GetObject(key, getObjectOptions...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	err := o.fs.Mkdir("")
	if err != nil {
		return err
	}
	modTime := src.ModTime()
	size := src.Size()

	metaMtimeStr := nsToFloatString(modTime.UnixNano())
	// Set the mtime in the meta data

	// Guess the content type
	mimeType := fs.MimeType(src)

	key := o.fs.root + o.remote

	putObjectOptions := make([]oss.Option, 0)
	putObjectOptions = append(putObjectOptions, oss.ContentType(mimeType))
	putObjectOptions = append(putObjectOptions, oss.Meta(metaMtime, metaMtimeStr))
	putObjectOptions = append(putObjectOptions, oss.ACL(oss.ACLType(o.fs.opt.ACL)))

	if !o.fs.opt.DisableChecksum && size > oss.MaxPartSize {
		hash, err := src.Hash(hash.MD5)

		if err == nil && matchMd5.MatchString(hash) {
			hashBytes, err := hex.DecodeString(hash)
			if err == nil {
				putObjectOptions = append(putObjectOptions, oss.Meta(metaMD5Hash, base64.StdEncoding.EncodeToString(hashBytes)))

			}
		}
	}

	if o.fs.opt.ServerSideEncryption != "" {
		putObjectOptions = append(putObjectOptions, oss.ServerSideEncryption(o.fs.opt.ServerSideEncryption))
	}
	if o.fs.opt.StorageClass != "" {
		putObjectOptions = append(putObjectOptions, oss.StorageClass(oss.StorageClassType(o.fs.opt.StorageClass)))
	}

	err = o.fs.bucket.PutObject(key, in, putObjectOptions...)
	if err != nil {
		return err
	}

	// Read the metadata from the newly created object
	o.meta = nil // wipe old metadata
	err = o.readMetaData()
	return err
}

// Remove an object
func (o *Object) Remove() error {
	key := o.fs.root + o.remote
	err := o.fs.bucket.DeleteObject(key)
	return err
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType() string {
	err := o.readMetaData()
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return ""
	}
	return o.mimeType
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.Copier      = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.ListRer     = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)
