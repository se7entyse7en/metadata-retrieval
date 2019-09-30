package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/src-d/metadata-retrieval/database"
	"github.com/src-d/metadata-retrieval/github"
	"github.com/src-d/metadata-retrieval/github/store"
	"golang.org/x/oauth2"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-log.v1"
)

// rewritten during the CI build step
var (
	version = "master"
	build   = "dev"
)

var app = cli.New("metadata", version, build, "GitHub metadata downloader")
var db *sql.DB

func main() {
	app.AddCommand(&Repository{})
	app.AddCommand(&Organization{})
	app.AddCommand(&Ghsync{})
	app.RunMain()
}

type DownloaderCmd struct {
	LogHTTP bool `long:"log-http" description:"log http requests (debug level)"`

	DB      string `long:"db" description:"PostgreSQL URL connection string, e.g. postgres://user:password@127.0.0.1:5432/ghsync?sslmode=disable"`
	Token   string `long:"token" short:"t" env:"GITHUB_TOKEN" description:"GitHub personal access token" required:"true"`
	Version int    `long:"version" description:"Version tag in the DB"`
	Cleanup bool   `long:"cleanup" description:"Do a garbage collection on the DB, deleting data from other versions"`
}

type Repository struct {
	cli.Command `name:"repo" short-description:"Download metadata for a GitHub repository" long-description:"Download metadata for a GitHub repository"`
	DownloaderCmd

	Owner string `long:"owner"  required:"true"`
	Name  string `long:"name"  required:"true"`
}

func (c *Repository) Execute(args []string) error {
	return c.ExecuteBody(
		log.New(log.Fields{"owner": c.Owner, "repo": c.Name}),
		func(logger log.Logger, httpClient *http.Client, downloader downloader) error {
			return downloader.DownloadRepository(context.TODO(), c.Owner, c.Name, c.Version)
		})
}

type Organization struct {
	cli.Command `name:"org" short-description:"Download metadata for a GitHub organization" long-description:"Download metadata for a GitHub organization"`
	DownloaderCmd

	Name string `long:"name" description:"GitHub organization name" required:"true"`
}

func (c *Organization) Execute(args []string) error {
	return c.ExecuteBody(
		log.New(log.Fields{"org": c.Name}),
		func(logger log.Logger, httpClient *http.Client, downloader downloader) error {
			return downloader.DownloadOrganization(context.TODO(), c.Name, c.Version)
		})
}

type Ghsync struct {
	cli.Command `name:"ghsync" short-description:"Mimics ghsync deep command" long-description:"Mimics ghsync deep command"`
	DownloaderCmd

	Name               string `long:"name" description:"GitHub organization name" required:"true"`
	NoForks            bool   `long:"no-forks" env:"GHSYNC_NO_FORKS" description:"github forked repositories will be skipped"`
	MaxParallelization int    `long:"max-parallelization" default:"10" env:"GHSYNC_MAX_PARALLELIZATION" description:"maximum number of repositories to download concurrently"`
}

func (c *Ghsync) Execute(args []string) error {
	return c.ExecuteBody(
		log.New(log.Fields{"org": c.Name}),
		func(logger log.Logger, httpClient *http.Client, downloader downloader) error {
			repos, err := listRepositories(context.TODO(), httpClient, c.Name, c.NoForks)
			if err != nil {
				return err
			}

			logger.Infof("downloading organization %s", c.Name)
			err = downloader.DownloadOrganization(context.TODO(), c.Name, c.Version)
			if err != nil {
				return fmt.Errorf("failed to download organization %v: %v", c.Name, err)
			}

			logger.Infof("finished downloading organization %s", c.Name)

			var wg sync.WaitGroup

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sem := make(chan struct{}, c.MaxParallelization)
			errCh := make(chan error, c.MaxParallelization)

			for _, repo := range repos {
				sem <- struct{}{}
				if ctx.Err() != nil {
					break
				}

				wg.Add(1)

				go func(logger log.Logger, repo string) {
					defer func() {
						<-sem
						wg.Done()
					}()

					logger.Infof("downloading repository")
					err := c.downloadRepository(ctx, downloader, repo)
					if err != nil {
						logger.Errorf(err, "error downloading repository")
						errCh <- err
						if ctx.Err() == nil {
							logger.Errorf(err, "canceling context to stop running jobs")
							cancel()
						}

						return
					}

					logger.Infof("finished downloading repository")
				}(logger.With(log.Fields{"repo": repo}), repo)
			}

			wg.Wait()

			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		})
}

func (c *Ghsync) downloadRepository(ctx context.Context, downloader downloader, repo string) error {
	err := downloader.DownloadRepository(ctx, c.Name, repo, c.Version)
	if err != nil {
		return fmt.Errorf("failed to download repository %v/%v: %v", c.Name, repo, err)
	}

	return nil
}

type bodyFunc = func(logger log.Logger, httpClient *http.Client, downloader downloader) error

func (c *DownloaderCmd) ExecuteBody(logger log.Logger, fn bodyFunc) error {
	client := oauth2.NewClient(context.TODO(), oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: c.Token},
	))

	if c.LogHTTP {
		setLogTransport(client, logger)
	}

	downloader, err := c.buildDownloader(client)
	if err != nil {
		return err
	}

	defer func() {
		db.Close()
	}()

	rate0, err := downloader.RateRemaining(context.TODO())
	if err != nil {
		return err
	}
	t0 := time.Now()

	err = fn(logger, client, downloader)
	if err != nil {
		return err
	}

	err = downloader.SetCurrent(c.Version)
	if err != nil {
		return err
	}

	if c.Cleanup {
		return downloader.Cleanup(c.Version)
	}

	elapsed := time.Since(t0)

	rate1, err := downloader.RateRemaining(context.TODO())
	if err != nil {
		return err
	}
	rateUsed := rate0 - rate1

	logger.With(log.Fields{"rate-limit-used": rateUsed, "total-elapsed": elapsed}).Infof("All metadata fetched")

	return nil
}

type downloader interface {
	DownloadRepository(ctx context.Context, owner string, name string, version int) error
	DownloadOrganization(ctx context.Context, name string, version int) error
	RateRemaining(ctx context.Context) (int, error)
	SetCurrent(version int) error
	Cleanup(currentVersion int) error
}

type multiDownloader struct {
	downloaderFactory func(httpClient *http.Client) (*github.Downloader, error)
	httpClient        *http.Client
	dbStore           *store.DB
}

func newMultiDownloader(httpClient *http.Client, db *sql.DB) *multiDownloader {
	dbStore := &store.DB{DB: db}
	return &multiDownloader{
		downloaderFactory: func(httpClient *http.Client) (*github.Downloader, error) {
			return github.NewDownloader(httpClient, db)
		},
		httpClient: httpClient,
		dbStore:    dbStore,
	}
}

func (md *multiDownloader) DownloadOrganization(ctx context.Context, name string, version int) error {
	downloader, err := md.downloaderFactory(md.httpClient)
	if err != nil {
		return err
	}

	err = downloader.DownloadOrganization(ctx, name, version)
	if err != nil {
		return err
	}

	return nil
}

func (md *multiDownloader) DownloadRepository(ctx context.Context, owner string, name string, version int) error {
	downloader, err := md.downloaderFactory(md.httpClient)
	if err != nil {
		return err
	}

	err = downloader.DownloadRepository(ctx, owner, name, version)
	if err != nil {
		return err
	}

	return nil
}

func (md *multiDownloader) RateRemaining(ctx context.Context) (int, error) {
	downloader, err := md.downloaderFactory(md.httpClient)
	if err != nil {
		return 0, err
	}

	r, err := downloader.RateRemaining(ctx)
	if err != nil {
		return 0, err
	}

	return r, nil
}

func (md *multiDownloader) SetCurrent(version int) error {
	err := md.dbStore.SetActiveVersion(version)
	if err != nil {
		return fmt.Errorf("failed to set current DB version to %v: %v", version, err)
	}
	return nil
}

func (md *multiDownloader) Cleanup(currentVersion int) error {
	err := md.dbStore.Cleanup(currentVersion)
	if err != nil {
		return fmt.Errorf("failed to do cleanup for DB version %v: %v", currentVersion, err)
	}
	return nil
}

func (c *DownloaderCmd) buildDownloader(client *http.Client) (downloader, error) {
	if c.DB == "" {
		log.Infof("using stdout to save the data")
		return github.NewStdoutDownloader(client)
	}

	var err error
	db, err = sql.Open("postgres", c.DB)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	if err = database.Migrate(c.DB); err != nil && err != migrate.ErrNoChange {
		return nil, err
	}

	return newMultiDownloader(client, db), nil
}
