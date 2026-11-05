// Package errs holds sentinel errors shared by the storage layers.
package errs

import "errors"

// ErrNotFound means the object does not exist in the backing store.
var ErrNotFound = errors.New("not found")
