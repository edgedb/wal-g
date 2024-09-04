package s3

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/fsnotify/fsnotify"
	"github.com/wal-g/tracelog"
)

// FileCredentialsProvider is a custom provider that loads credentials
// from a file and reloads them when the file is modified.
type FileCredentialsProvider struct {
	filename      string
	creds         *credentials.Credentials
	credsMutex    sync.RWMutex
	watcher       *fsnotify.Watcher
	reloadTimeout time.Duration
}

func NewFileCredentialsProvider(filename string) (*FileCredentialsProvider, error) {
	provider := &FileCredentialsProvider{
		filename:      filename,
		reloadTimeout: 1 * time.Second,
	}

	// Load initial credentials
	if err := provider.loadCredentials(); err != nil {
		return nil, err
	}

	// Set up file watcher
	if err := provider.setupWatcher(); err != nil {
		return nil, err
	}

	return provider, nil
}

func (p *FileCredentialsProvider) Retrieve() (credentials.Value, error) {
	p.credsMutex.RLock()
	defer p.credsMutex.RUnlock()

	return p.creds.Get()
}

func (p *FileCredentialsProvider) IsExpired() bool {
	p.credsMutex.RLock()
	defer p.credsMutex.RUnlock()

	return p.creds.IsExpired()
}

func (p *FileCredentialsProvider) loadCredentials() error {
	p.credsMutex.Lock()
	defer p.credsMutex.Unlock()

	creds := credentials.NewSharedCredentials(p.filename, "")
	if _, err := creds.Get(); err != nil {
		return err
	}

	p.creds = creds
	return nil
}

func (p *FileCredentialsProvider) setupWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	p.watcher = watcher

	tracelog.InfoLogger.Printf("watching %q for changes", p.filename)

	dir, _ := filepath.Split(p.filename)
	if err := watcher.Add(dir); err != nil {
		return err
	}

	go p.watchFile()

	return nil
}

func (p *FileCredentialsProvider) watchFile() {
	for {
		select {
		case event, ok := <-p.watcher.Events:
			if !ok {
				// Channel closed.
				return
			}
			_, eventFn := filepath.Split(event.Name)
			_, watchedFn := filepath.Split(p.filename)
			if eventFn == watchedFn {
				time.Sleep(p.reloadTimeout) // Debounce to prevent multiple reloads
				if err := p.loadCredentials(); err != nil {
					tracelog.ErrorLogger.Printf("could not reload credentials: %v", err)
				} else {
					tracelog.InfoLogger.Printf("credentials reloaded")
				}
			}
		case err, ok := <-p.watcher.Errors:
			if !ok {
				// Channel closed.
				return
			}
			tracelog.ErrorLogger.Printf("error while watching the credentials file: %v", err)
		}
	}
}

func (p *FileCredentialsProvider) Close() error {
	return p.watcher.Close()
}
