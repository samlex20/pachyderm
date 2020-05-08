package testing

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/testpachd"
	"golang.org/x/sync/errgroup"
)

func writeTestFile(t *testing.T, tw *tar.Writer, testFile string) {
	hdr := &tar.Header{
		Name: "/" + testFile,
		Size: int64(len(testFile)),
	}
	require.NoError(t, tw.WriteHeader(hdr))
	_, err := tw.Write([]byte(testFile))
	require.NoError(t, err)
	require.NoError(t, tw.Flush())
}

func TestCompaction(t *testing.T) {
	config := &serviceenv.PachdFullConfiguration{}
	config.NewStorageLayer = true
	config.StorageMemoryThreshold = 20
	config.StorageShardThreshold = 20
	config.StorageLevelZeroSize = 10
	config.StorageGCPolling = "3s"
	testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
		c := env.PachClient
		repo := "test"
		require.NoError(t, c.CreateRepo(repo))
		var eg errgroup.Group
		for i := 0; i < 3; i++ {
			branch := "test" + strconv.Itoa(i)
			eg.Go(func() error {
				var commit *pfs.Commit
				var testFiles []string
				t.Run("PutTar", func(t *testing.T) {
					var err error
					for i := 0; i < 10; i++ {
						commit, err = c.StartCommit(repo, branch)
						require.NoError(t, err)
						buf := &bytes.Buffer{}
						tw := tar.NewWriter(buf)
						// Create files.
						for j := 0; j < 10; j++ {
							testFile := strconv.Itoa(i*10 + j)
							writeTestFile(t, tw, testFile)
							testFiles = append(testFiles, testFile)
						}
						require.NoError(t, tw.Close())
						require.NoError(t, c.PutTar(repo, commit.ID, buf))
						require.NoError(t, c.FinishCommit(repo, commit.ID))
					}
				})
				getTarContent := func(r io.Reader) string {
					tr := tar.NewReader(r)
					_, err := tr.Next()
					require.NoError(t, err)
					buf := &bytes.Buffer{}
					_, err = io.Copy(buf, tr)
					require.NoError(t, err)
					return buf.String()
				}
				t.Run("GetTar", func(t *testing.T) {
					r, err := c.GetTar(repo, commit.ID, "/0")
					require.NoError(t, err)
					require.Equal(t, "0", getTarContent(r))
					r, err = c.GetTar(repo, commit.ID, "/50")
					require.NoError(t, err)
					require.Equal(t, "50", getTarContent(r))
					r, err = c.GetTar(repo, commit.ID, "/99")
					require.NoError(t, err)
					require.Equal(t, "99", getTarContent(r))
				})
				t.Run("GetTarConditional", func(t *testing.T) {
					downloadProb := 0.25
					require.NoError(t, c.GetTarConditional(repo, commit.ID, "/*", func(fileInfo *pfs.FileInfoNewStorage, r io.Reader) error {
						if rand.Float64() < downloadProb {
							require.Equal(t, strings.TrimPrefix(fileInfo.File.Path, "/"), getTarContent(r))
						}
						return nil
					}))
				})
				t.Run("GetTarConditionalDirectory", func(t *testing.T) {
					fullBuf := &bytes.Buffer{}
					fullTw := tar.NewWriter(fullBuf)
					rootHdr := &tar.Header{
						Typeflag: tar.TypeDir,
						Name:     "/",
					}
					require.NoError(t, fullTw.WriteHeader(rootHdr))
					sort.Strings(testFiles)
					for _, testFile := range testFiles {
						writeTestFile(t, fullTw, testFile)
					}
					require.NoError(t, fullTw.Close())
					require.NoError(t, c.GetTarConditional(repo, commit.ID, "/", func(fileInfo *pfs.FileInfoNewStorage, r io.Reader) error {
						buf := &bytes.Buffer{}
						_, err := io.Copy(buf, r)
						require.NoError(t, err)
						require.Equal(t, 0, bytes.Compare(fullBuf.Bytes(), buf.Bytes()))
						return nil
					}))
				})
				return nil
			})
		}
		return eg.Wait()
	}, config)
}

func TestListFileNSTwoCommits(t *testing.T) {
	config := &serviceenv.PachdFullConfiguration{}
	config.NewStorageLayer = true
	t.Parallel()
	err := testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numFiles := 5

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.PutFile(repo, commit1.ID, fmt.Sprintf("file%d", i), strings.NewReader("foo\n"))
			require.NoError(t, err)
		}

		fileInfos, err := env.PachClient.ListFileNS(repo, "master", "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fileInfos))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.PutFile(repo, commit2.ID, fmt.Sprintf("file2-%d", i), strings.NewReader("foo\n"))
			require.NoError(t, err)
		}

		fileInfos, err = env.PachClient.ListFileNS(repo, commit2.ID, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fileInfos))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		fileInfos, err = env.PachClient.ListFileNS(repo, commit1.ID, "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fileInfos))

		fileInfos, err = env.PachClient.ListFileNS(repo, commit2.ID, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fileInfos))

		return nil
	}, config)
	require.NoError(t, err)
}

func TestListFileNS(t *testing.T) {
	config := &serviceenv.PachdFullConfiguration{}
	config.NewStorageLayer = true
	t.Parallel()
	err := testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		fileContent1 := "foo\n"
		_, err = env.PachClient.PutFile(repo, commit.ID, "dir/foo", strings.NewReader(fileContent1))
		require.NoError(t, err)

		fileContent2 := "bar\n"
		_, err = env.PachClient.PutFile(repo, commit.ID, "dir/bar", strings.NewReader(fileContent2))
		require.NoError(t, err)

		fileInfos, err := env.PachClient.ListFileNS(repo, commit.ID, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
		require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		fileInfos, err = env.PachClient.ListFileNS(repo, commit.ID, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
		require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")

		return nil
	}, config)
	require.NoError(t, err)
}

func TestListFileNS2(t *testing.T) {
	config := &serviceenv.PachdFullConfiguration{}
	config.NewStorageLayer = true
	t.Parallel()
	err := testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent))
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent))
		require.NoError(t, err)

		fileInfos, err := env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/3", strings.NewReader(fileContent))
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir/2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		return nil
	}, config)
	require.NoError(t, err)
}

func TestListFileNS3(t *testing.T) {
	config := &serviceenv.PachdFullConfiguration{}
	config.NewStorageLayer = true
	t.Parallel()
	err := testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent))
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent))
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err := env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/3/foo", strings.NewReader(fileContent))
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "dir/3/bar", strings.NewReader(fileContent))
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir/3/bar")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.PutFile(repo, "master", "file", strings.NewReader(fileContent))
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileNS(repo, "master", "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		return nil
	}, config)
	require.NoError(t, err)
}
