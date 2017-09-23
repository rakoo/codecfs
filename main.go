package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var allSizes sync.Map
var allFiles sync.Map

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Missing input dir")
	}

	fuse.Unmount("/tmp/codecfs")
	err := os.Mkdir("/tmp/codecfs", os.ModeDir|0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	} else if os.IsExist(err) {
		os.Chmod("/tmp/codecfs", os.ModeDir|0755)
	}
	c, err := fuse.Mount(
		"/tmp/codecfs",
		fuse.FSName("codecfs"),
		fuse.Subtype("codecfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("Codec filesystem"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	srv := fs.New(c, nil)
	root := &Root{os.Args[1]}
	if err := srv.Serve(root); err != nil {
		log.Fatal(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}

	fuse.Unmount("/tmp/codecfs")
}

var _ fs.HandleReadDirAller = &Root{}
var _ fs.NodeStringLookuper = &Root{}

type Root struct {
	dir string
}

func (r *Root) Root() (fs.Node, error) {
	return r, nil
}

func (r *Root) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func (r *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{
		fuse.Dirent{
			Inode: 2,
			Type:  fuse.DT_Dir,
			Name:  "ogg",
		},
	}, nil
}

func (r *Root) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == "ogg" {
		return &dir{
			dir:     r.dir,
			encoder: "ogg",
		}, nil
	}

	return nil, fuse.ENOENT
}

var _ fs.HandleReadDirAller = &dir{}
var _ fs.NodeStringLookuper = &dir{}

type dir struct {
	dir     string
	encoder string
}

func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir, err := os.Open(d.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	ents, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}
	out := make([]fuse.Dirent, 0, len(ents))
	for _, ent := range ents {
		if !ent.Mode().IsDir() && !ent.Mode().IsRegular() {
			continue
		}
		var typ fuse.DirentType
		switch {
		case ent.Mode().IsDir():
			typ = fuse.DT_Dir
		case ent.Mode().IsRegular():
			typ = fuse.DT_File
		}

		name := ent.Name()
		if typ == fuse.DT_File && isAudio(filepath.Join(d.dir, ent.Name())) {
			ext := filepath.Ext(name)
			name = strings.Replace(name, ext, ".ogg", 1)
			if _, err := os.Stat(filepath.Join(d.dir, name)); os.IsNotExist(err) {
				allFiles.Store(filepath.Join(d.dir, name), filepath.Join(d.dir, ent.Name()))
			}
		}
		out = append(out, fuse.Dirent{
			Type: typ,
			Name: name,
		})
	}
	return out, nil
}

func isAudio(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	var buf [512]byte
	_, err = io.ReadFull(file, buf[:])
	if err != nil && err != io.EOF {
		return false
	}

	// From spec (https://mimesniff.spec.whatwg.org/):
	//
	// ```
	// An audio or video type
	// is any parsable MIME type where type is equal to "audio" or "video"
	// or where the MIME type portion is equal to one of the following:
	//
	//     application/ogg
	// ```
	//
	// As an addendum, files ending with a .flac will be considered valid
	// audio
	contentType := http.DetectContentType(buf[:])
	if strings.HasPrefix(contentType, "audio/") ||
		strings.HasPrefix(contentType, "video/") ||
		contentType == "application/ogg" ||
		strings.HasSuffix(path, ".flac") {
		return true
	}
	return false
}

func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	baseNameString := filepath.Join(d.dir, name)
	if _, err := os.Stat(baseNameString); os.IsNotExist(err) {
		// Note: This works if the user explores files and we do a conversion
		// of name. If the user directly goes to a specific file without any
		// other interaction before, then we don't know what files to map back
		// to.
		baseName, ok := allFiles.Load(baseNameString)
		if ok {
			baseNameString = baseName.(string)
		}
	}
	ford, err := os.Open(baseNameString)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}
		return nil, err
	}
	stat, err := ford.Stat()
	if err != nil {
		return nil, err
	}
	switch {
	case stat.Mode().IsDir():
		return &dir{
			dir:     baseNameString,
			encoder: d.encoder,
		}, nil
	case stat.Mode().IsRegular():
		return &file{
			name:    baseNameString,
			encoder: d.encoder,
		}, nil
	}
	return nil, fuse.ENOENT
}

var _ fs.NodeOpener = &file{}

type file struct {
	name    string
	encoder string
}

func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0555

	// Get from cache
	realSize, ok := allSizes.Load(f.name)
	if ok {
		a.Size = realSize.(uint64)
		return nil
	}

	// Get from original file, if it exists as-is
	stat, err := os.Stat(f.name)
	if err == nil {
		a.Size = uint64(stat.Size())
		return nil
	}

	// Make up encoded cache size
	if os.IsNotExist(err) {
		baseName, ok := allFiles.Load(f.name)
		if ok {
			stat, err = os.Stat(baseName.(string))
			if err != nil {
				return err
			}

			// We lie about the size. In a typical usecase we do lossy encodes, so
			// the output size should be smaller than the input size. By making
			// the fake size bigger, we should make everyone happy.
			a.Size = 10 * uint64(stat.Size())

		}
	}
	return nil
}

func (f *file) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if file, err := os.Open(f.name); err == nil {
		return nativeFile{file}, nil
	}

	cmdArgs := []string{
		"-i",
		f.name,
		"-f",
		"ogg",
		"-",
	}
	ffmpeg := exec.CommandContext(context.Background(), "ffmpeg", cmdArgs...)
	stdoutPipe, err := ffmpeg.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = ffmpeg.Start()
	if err != nil {
		return nil, err
	}

	return &fileHandle{
		name:    f.name,
		close:   ffmpeg.Wait,
		pipe:    stdoutPipe,
		buffer:  bytes.Buffer{},
		encoder: f.encoder,
	}, nil
}

var _ fs.HandleReader = &fileHandle{}
var _ fs.HandleReleaser = &fileHandle{}

type fileHandle struct {
	name    string
	close   func() error
	pipe    io.ReadCloser
	buffer  bytes.Buffer
	encoder string
}

func (fh *fileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.close()
}

func (fh *fileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if int64(fh.buffer.Len()) < req.Offset+int64(req.Size) {
		// Fill buffer
		_, err := io.CopyN(&fh.buffer, fh.pipe, req.Offset+int64(req.Size)-int64(fh.buffer.Len()))
		if err != nil && err != io.EOF {
			return err
		}
	}

	var min int64
	if req.Offset > int64(fh.buffer.Len()) {
		min = int64(fh.buffer.Len())
	} else {
		min = req.Offset
	}

	var max int64
	if req.Offset+int64(req.Size) > int64(fh.buffer.Len()) {
		max = int64(fh.buffer.Len())
	} else {
		max = req.Offset + int64(req.Size)
	}

	resp.Data = make([]byte, req.Size)
	n := copy(resp.Data[:], fh.buffer.Bytes()[min:max])

	// Help applications to know that there's nothing coming after that
	if n == 0 {
		allSizes.Store(fh.name, uint64(fh.buffer.Len()))
		return io.EOF
	}
	return nil
}

type nativeFile struct {
	*os.File
}

var _ fs.HandleReader = nativeFile{}
var _ fs.HandleReleaser = nativeFile{}

func (f nativeFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	resp.Data = make([]byte, req.Size)
	n, err := f.ReadAt(resp.Data, req.Offset)
	resp.Data = resp.Data[:n]
	if err == io.EOF {
		err = nil
	}
	return err
}

func (f nativeFile) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return f.Close()
}
