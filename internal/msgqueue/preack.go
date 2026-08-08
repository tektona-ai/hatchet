package msgqueue

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsPermanentPreAckError reports whether a pre-ack handler failure is one that
// no amount of redelivery can fix, so the message should be dropped rather than
// retried forever.
//
// It lives in the msgqueue package rather than a single backend because every
// durable implementation has to make the same call at the same point in the
// ack sequence, and a classification that drifts between backends would mean
// the same poison message loops on one broker and drains on another.
func IsPermanentPreAckError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// invalid input syntax for type json / jsonb
		if pgErr.Code == pgerrcode.InvalidTextRepresentation {
			return true
		}
	}

	// Fallback: some error paths may lose pg error type info.
	errStr := err.Error()
	if strings.Contains(errStr, fmt.Sprintf("SQLSTATE %s", pgerrcode.InvalidTextRepresentation)) {
		return true
	}

	if strings.Contains(errStr, "invalid input syntax for type json") {
		return true
	}

	return false
}
