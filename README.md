# Databases
Work for an independent study on the implementation of high performance databases and distributed databases.

## LogDatabase
`LogDatabase/` is a simple implementation of a log based key-value pair database. Data is always inserted at the end of a file. A mapping of `key` to `file offset for latest ocurrence of key` is maintained for fast reads. The underlying file uses the following custom binary format: 

**For each entry**
- 4 bytes for the key size
- 4 bytes for the value size
- key
- value

As entries are only appended instead of modified, the underlying file can grow indefinitely in size. To combat this, when the file exceeds a certain size the database will "compact" the file by removing duplicate entries.


