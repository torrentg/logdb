# journal

A simple log-structured library for event-driven applications.

Journal is essentially an append-only data file (\*.dat) with an index file (\*.idx) used to speed up lookups.
No complex data structures, no sophisticated algorithms, only basic file access.
We rely on the filesystem cache (managed by the operating system) to ensure read performance.

Main features:

* Variable length record type
* Records uniquely identified by a sequential number (seqnum)
* Records are indexed by timestamp (monotonic non-decreasing field)
* There are no other indexes other than seqnum and timestamp
* Records can be appended, read, and searched
* Records cannot be updated or deleted
* Allows reverting the last entries (rollback)
* Allows removing obsolete entries (purge)
* Supports read-write concurrency (multi-thread)
* Automatic data recovery in case of catastrophic events
* Minimal memory footprint
* No dependencies

## File format

Journal file formats (`.dat`, `.idx`) use native host byte order for integer fields (endianness).
Files copied to a machine with different endianness may not be readable.

### dat file format

```txt
     header       record1          data1          record2       data2
┌──────┴──────┐┌─────┴─────┐┌────────┴────────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum1        raw bytes 1      seqnum2     raw bytes 2
  format         timestamp1                      timestamp2
  metadata       checksum1                       checksum2
                 length1                         length2
```

### idx file format

```txt
     header      record1       record2
┌──────┴──────┐┌─────┴─────┐┌─────┴─────┐...
  magic number   seqnum1      seqnum2
  format         timestamp1   timestamp2
                 pos1         pos2
```

## Usage

Drop [`journal.h`](journal.h) and [`journal.c`](journal.c) into your project and start using it.

```c
#include "journal.h"

#define MAX_ENTRIES 100

ldb_journal_t *journal = NULL;
ldb_entry_t entries[MAX_ENTRIES] = {{0}};
ldb_stats_t stats = {0};
char buf[10 * 1024];
size_t num = 0;
int rc = 0;

journal = ldb_alloc();
ldb_open(journal, "/my/directory", "example", LDB_OPEN_CREATE);

...

rc = ldb_append(journal, entries, MAX_ENTRIES, NULL);

if (rc != LDB_OK)
    printf("Error: %s\n", ldb_strerror(rc));

...

ldb_stats(journal, 0, UINT64_MAX, &stats);

print("Number of entries = %zu\n", stats.data_size);
print("Min seqnum = %zu\n", stats.min_seqnum);
print("Max seqnum = %zu\n", stats.max_seqnum);
print("Data bytes = %zu\n", stats.data_size);

rc = ldb_read(journal, stats.min_seqnum, entries, MAX_ENTRIES, buf, sizeof(buf), &num);

if (rc == LDB_OK)
{
    if (num == MAX_ENTRIES)
        printf("Read all requested data\n");
    else if (entries[num].seqnum == 0)
        printf("Last record reached\n");
    else {
        printf("Not enough memory in buffer, only read %zu entries\n", num);
        printf("Next entry requires a minimum of %zu bytes\n", entries[num].data_len + 24);
    }

    for (int i = 0; i < num; i++) {
        printf("entry[%zu].data = %s\n", entry[i].seqnum, entry[i].data);
    }
}

ldb_close(journal);
ldb_free(journal);
```

Read the function documentation in `journal.h`.<br/>
See [`tests.c`](tests.c) for unit tests.<br/>
See [`example.c`](example.c) for basic function usage.<br/>
See [`performance.c`](performance.c) for concurrent usage.<br/>
See [`journalctl.c`](journalctl.c) for a maintenance tool.

## Contributors

| Name | Contribution |
|:-----|:-------------|
| [Gerard Torrent](https://github.com/torrentg/) | Initial work<br/>Code maintainer|
| [J_H](https://codereview.stackexchange.com/users/145459/j-h) | [Code review ](https://codereview.stackexchange.com/questions/291660/a-c-header-only-log-structured-database) |
| [Harith](https://codereview.stackexchange.com/users/265278/harith) | [Code review ](https://codereview.stackexchange.com/questions/291660/a-c-header-only-log-structured-database) |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
