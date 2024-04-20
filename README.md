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
* Data insertion flush data to disk
* Automatic data recovery on catastrofic event
* Records can be read (retrieved by seqnum)
* Records can be searched by id (seqnum)
* Records can be searched by timestamp
* Rollback means to remove X records from top
* Can be purged (removing X records from bottom)

### dat file format

```
     header      record1        data1       record2        data2
┌──────┴──────┐┌─────┴─────┐┌─────┴─────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum       raw bytes    seqnum       raw bytes
  format         timestamp                 timestamp
  etc.           lenght                    lenght
```

### idx file format

```
     header      record1       record2
┌──────┴──────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum       seqnum
  format         timestamp    timestamp
  etc.           pos          pos
```

## Usage

Drop off [`logdb.hpp`](logdb.hpp) in your project and start using it.

`#define LDB_IMPL`

TODO

## Contributors

| Name | Contribution |
|:-----|:-------------|
| [Gerard Torrent](https://github.com/torrentg/) | Initial work<br/>Code maintainer|

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
