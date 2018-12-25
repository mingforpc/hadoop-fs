package herror

import "errors"

// ErrEOF End of file
var ErrEOF = errors.New("End of file")

// ErrNoFound File no found
var ErrNoFound = errors.New("File no found")

// ErrExist File exists
var ErrExist = errors.New("File exists")

// ErrAccess Permission denied
var ErrAccess = errors.New("Permission denied")

// ErrAgain Try again
var ErrAgain = errors.New("Try again")

// ErrNoAttr XATTR_REPLACE was specified, and the attribute does not exist
var ErrNoAttr = errors.New("XATTR_REPLACE was specified, and the attribute does not exist")

// ErrNotsup The namespace prefix of name is not valid.
var ErrNotsup = errors.New("The namespace prefix of name is not valid")

// ErrRange Math result not representable
var ErrRange = errors.New("Math result not representable")
