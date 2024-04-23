# logdb

A simple log-structured database.

It is an header-only C embeded database with no dependencies. 
The logdb goal is to cover the following case:

* Need to persist sequentially ordered data
* Most operations are write type
* Data is rarely read or searched
* Allow to revert last entries (rollback)
* Eventually purge obsolete entries (purge)
* Minimal memory footprint

## Description

Logdb is a simple database with the following characteristics:

* Records have variable length (non-fixed record size)
* Record identifier is a sequential number
* Record are indexed by timestamp (monotonic non-decreasing field)
* Only append function is supported (no update, no delete)
* Just after insertion data is flushed to disk (no delayed writes)
* Automatic data recovery on catastrofic event
* Records can be read (retrieved by seqnum)
* Records can be searched by id (seqnum)
* Records can be searched by timestamp
* Rollback means to remove X records from top
* Can be purged (removing X records from bottom)

### dat file format

```
     header       record1          data1          record2       data2
┌──────┴──────┐┌─────┴─────┐┌────────┴────────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum1        raw bytes 1      seqnum2     raw bytes 2
  format         timestamp1                      timestamp2
  etc            checksum1                       checksum2
                 length1                         length2
```

### idx file format

```
     header      record1       record2
┌──────┴──────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum1      seqnum2
  format         timestamp1   timestamp2
  etc            pos1         pos2
```

## Usage

Drop off [`logdb.h`](logdb.h) in your project and start using it.

`#define LDB_IMPL`

TODO

## Contributors

| Name | Contribution |
|:-----|:-------------|
| [Gerard Torrent](https://github.com/torrentg/) | Initial work<br/>Code maintainer|
| [J_H](https://codereview.stackexchange.com/users/145459/j-h) | [Code review ](https://codereview.stackexchange.com/questions/291660/a-c-header-only-log-structured-database) |
| [Harith](https://codereview.stackexchange.com/users/265278/harith) | [Code review ](https://codereview.stackexchange.com/questions/291660/a-c-header-only-log-structured-database) |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
