package sqlextractor

import "errors"

var (
	errMaxRetriesReached = errors.New("exceeded retry limit")
)

// TryFunc represents functions that can be retried.
type tryFunc func(attempt int) (retry bool, err error)

// Do keeps trying the function until the second argument
// returns false, or no error is returned.
func try(max int, fn tryFunc) error {
	var (
		err     error
		retry   bool
		attempt int
	)

	for {
		retry, err = fn(attempt)
		if !retry || err == nil {
			break
		}

		attempt++
		if attempt > max {
			return errMaxRetriesReached
		}
	}

	return err
}
