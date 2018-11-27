package hadoop_error

import "errors"

var EOF = errors.New("End of file")
var NO_FOUND = errors.New("File no found")
var EEXIST = errors.New("File exists")
var EACCES = errors.New("Permission denied")
var EAGAIN = errors.New("Try again")
var ENOATTR = errors.New("XATTR_REPLACE was specified, and the attribute does not exist.")
var ENOTSUP = errors.New("The namespace prefix of name is not valid.")
var ERANGE = errors.New("Math result not representable")
