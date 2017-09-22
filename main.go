package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var allSizes sync.Map

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
		out = append(out, fuse.Dirent{
			Type: typ,
			Name: ent.Name(),
		})
	}
	return out, nil
}

func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	ford, err := os.Open(filepath.Join(d.dir, name))
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
			dir:     filepath.Join(d.dir, name),
			encoder: d.encoder,
		}, nil
	case stat.Mode().IsRegular():
		return &file{
			name:    filepath.Join(d.dir, name),
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

	realSize, ok := allSizes.Load(f.name)
	if ok {
		a.Size = realSize.(uint64)
	} else {
		// We lie about the size. In a typical usecase we do lossy encodes, so
		// the output size should be smaller than the input size. By making
		// the fake size bigger, we should make everyone happy.
		stat, err := os.Stat(f.name)
		if err != nil {
			return err
		}
		a.Size = uint64(10 * stat.Size())
	}
	return nil
}

func (f *file) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
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
