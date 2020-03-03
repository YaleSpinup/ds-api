package s3datarepository

import (
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

type stop struct {
	error
}

// retry is stolen from https://upgear.io/blog/simple-golang-retry-function/
func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}

		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2

			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}

// rollBack executes functions from a stack of rollback functions
func rollBack(t *[]func() error) {
	if t == nil {
		return
	}

	tasks := *t
	log.Errorf("executing rollback of %d tasks", len(tasks))
	for i := len(tasks) - 1; i >= 0; i-- {
		f := tasks[i]
		if funcerr := f(); funcerr != nil {
			log.Errorf("rollback task error: %s, continuing rollback", funcerr)
		}
	}
}
