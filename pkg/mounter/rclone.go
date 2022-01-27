package mounter

import (
	"fmt"
	"os"
	"path"

	"github.com/ctrox/csi-s3/pkg/s3"
	"github.com/golang/glog"
)

// Implements Mounter
type rcloneMounter struct {
	meta            *s3.FSMeta
	url             string
	region          string
	accessKeyID     string
	secretAccessKey string
}

const (
	rcloneCmd = "rclone"
)

func newRcloneMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &rcloneMounter{
		meta:            meta,
		url:             cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
	}, nil
}

func (rclone *rcloneMounter) Stage(stageTarget string) error {
	return nil
}

func (rclone *rcloneMounter) Unstage(stageTarget string) error {
	return nil
}

func (rclone *rcloneMounter) Mount(source string, target string) error {
	args := []string{
		"mount",
		fmt.Sprintf(":s3:%s", path.Join(rclone.meta.BucketName, rclone.meta.Prefix, rclone.meta.FSPath)),
		fmt.Sprintf("%s", target),
		"--daemon",
		"--s3-provider=AWS",
		"--s3-env-auth=true",
		fmt.Sprintf("--s3-region=%s", rclone.region),
		fmt.Sprintf("--s3-endpoint=%s", rclone.url),
		"--allow-other",
		// TODO: make this configurable
		// "--vfs-cache-mode=writes",

		// From here on its @Ceres Customizations:
		"--attr-timeout=1m0s",
		"--dir-cache-time=10m0s",
		"--max-read-ahead=1M",
		"--vfs-cache-mode=full",
		"--vfs-cache-poll-interval=5m0s",
		"--vfs-read-ahead=1M",
		"--vfs-write-back=1m0s",
		"--vfs-write-wait=10s",
		"--write-back-cache",
		"--use-server-modtime",

		// From Seth's Experiments:
		//
		// rclone mount \
		//   --allow-other \
		//   --attr-timeout 1m0s \
		//   --dir-cache-time 10m0s \
		//   --max-read-ahead 1Mi \
		//   --vfs-cache-mode full \
		//   --vfs-cache-poll-interval 5m0s \
		//   --vfs-read-ahead 1Mi \
		//   --vfs-write-back 1m0s \
		//   --vfs-write-wait 10s \
		//   --write-back-cache \
		//   --use-server-modtime \
		//   ceres-s3:ceres-flight-data-test \
		//   ceres-flight-data-test

		//   # --allow-other                            Allow access to other users
		//   # --allow-root                             Allow access to root user
		//   # --attr-timeout duration                  Time for which file/directory attributes are cached (default 1s)
		//   # --default-permissions                    Makes kernel enforce access control based on the file mode
		//   # --dir-cache-time duration                Time to cache directory entries for (default 5m0s)
		//   # --dir-perms FileMode                     Directory permissions (default 0777)
		//   # --file-perms FileMode                    File permissions (default 0666)
		//   # --gid uint32                             Override the gid field set by the filesystem (default 20)
		//   # --max-read-ahead SizeSuffix              The number of bytes that can be prefetched for sequential reads (not supported on Windows) (default 128Ki)
		//   # --uid uint32                             Override the uid field set by the filesystem (default 501)
		//   # --umask int                              Override the permission bits set by the filesystem (default 18)
		//   # --vfs-cache-mode CacheMode               Cache mode off|minimal|writes|full (default off)
		//   # --vfs-cache-poll-interval duration       Interval to poll the cache for stale objects (default 1m0s)
		//   # --vfs-read-ahead SizeSuffix              Extra read ahead over --buffer-size when using cache-mode full
		//   # --vfs-write-back duration                Time to writeback files after last use when using cache (default 5s)
		//   # --vfs-write-wait duration                Time to wait for in-sequence write before giving error (default 1s)
		//   # --write-back-cache                       Makes kernel buffer writes before sending them to rclone (without this, writethrough caching is used)
	}
	glog.Errorf("@CERES: calling `rclone mount` with args=", args)
	os.Setenv("AWS_ACCESS_KEY_ID", rclone.accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", rclone.secretAccessKey)
	return fuseMount(target, rcloneCmd, args)
}
