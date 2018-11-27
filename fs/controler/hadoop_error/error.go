package hadoop_error

import "errors"

var EOF = errors.New("End of file")
var NO_FOUND = errors.New("File no found")
var EEXIST = errors.New("File exists")
var EACCES = errors.New("Permission denied")
var EAGAIN = errors.New("Try again")
