#include <stdarg.h>
#include <stdlib.h>

#include "logdb.h"

#define MAX_ENTRIES 10

static const char lorem[] = 
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, " \
    "sed do eiusmod tempor incididunt ut labore et dolore magna " \
    "aliqua. Ut enim ad minim veniam, quis nostrud exercitation " \
    "ullamco laboris nisi ut aliquip ex ea commodo consequat. " \
    "Duis aute irure dolor in reprehenderit in voluptate velit " \
    "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint " \
    "occaecat cupidatat non proident, sunt in culpa qui officia " \
    "deserunt mollit anim id est laborum.";

ldb_entry_t create_random_entry(size_t seqnum, size_t timestamp) {
    return (ldb_entry_t) {
        .seqnum = seqnum,
        .timestamp = timestamp,
        .metadata = (char *) lorem + (rand() % (sizeof(lorem) - 11)),
        .metadata_len = 10,
        .data = (char *) lorem + (rand() % (sizeof(lorem) - 21)),
        .data_len = 20
    };
}

void print_entry(const char *prefix, const ldb_entry_t *entry) {
    printf("%s{ seqnum=%zu, timestamp=%zu, metadata='%.*s', data='%.*s' }\n", 
            prefix,
            entry->seqnum, entry->timestamp, 
            entry->metadata_len, entry->metadata, 
            entry->data_len, entry->data);
}

void print_result(const char *fmt, int rc, ...)
{
    char buf[1024] = {0};
    va_list args;

    va_start(args, rc);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    printf("%-65s: %s\n", buf, ldb_strerror(rc));
}

int run(ldb_db_t *db)
{
    ldb_stats_t stats = {0};
    size_t timestamp = 0;
    size_t seqnum1 = 0;
    size_t seqnum2 = 0;
    size_t num = 0;
    int rc = 0;

    // don't mix read and write entries, they have distinct memory-owner
    ldb_entry_t wentries[MAX_ENTRIES] = {{0}};
    ldb_entry_t rentries[MAX_ENTRIES] = {{0}};
    ldb_entry_t wentry = {0};
    ldb_entry_t rentry = {0};

    srand(time(NULL));

    // remove existing database
    remove("example.dat");
    remove("example.idx");

    // create an empty database
    rc = ldb_open(db, "", "example", true);
    print_result("open", rc);

    wentry = create_random_entry(1000, 42);
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append initial entry (sn=1000 and ts=42)", rc);

    wentry = create_random_entry(1001, 42);
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append entry with correlative seqnum", rc);

    wentry.seqnum = 999;
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append entry with non-correlative seqnum", rc);

    wentry.seqnum = 1002;
    wentry.timestamp = 40;
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append entry with timestamp less than previous", rc);

    wentry = create_random_entry(0, 43);
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append entry with seqnum = 0 (assigned next value, %zu)", rc, wentry.seqnum);

    wentry = create_random_entry(0, 0);
    rc = ldb_append(db, &wentry, 1, NULL);
    print_result("append entry with timestamp = 0 (assigned current millis)", rc);

    // you can enter a batch of entries (1 single flush is done at the end)
    for (size_t i = 0; i < MAX_ENTRIES; i++) {
        wentries[i] = create_random_entry(0, 0);
    }
    rc = ldb_append(db, wentries, MAX_ENTRIES, NULL);
    print_result("append 10 entries in a row", rc);

    /// timestamp of last entry
    timestamp = wentries[MAX_ENTRIES-1].timestamp;

    rc = ldb_read(db, 1001, &rentry, 1, NULL);
    print_result("read existing entry (sn=1001)", rc);

    rc = ldb_read(db, 9999, &rentry, 1, NULL);
    print_result("read non-existing entry (sn=9999)", rc);

    // you can read multiple entries in a row
    rc = ldb_read(db, 1010, rentries, MAX_ENTRIES, &num);
    print_result("read %d entries starting at 1010 (read-entries=%zu)", rc, MAX_ENTRIES, num);

    rc = ldb_stats(db, 0, 9999, &stats);
    print_result("stats range [0-9999] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_stats(db, 1005, 1011, &stats);
    print_result("stats range [1005-1011] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_stats(db, 0, 100, &stats);
    print_result("stats range [0-100] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_search(db, 0, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(db, 0, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=0 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(db, 42, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(db, 42, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=42 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(db, 1000, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(db, 1000, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=1000 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(db, timestamp, LDB_SEARCH_LOWER, &seqnum1);
    print_result("search ts=%zu, mode=lower", rc, timestamp);

    rc = ldb_search(db, timestamp, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=%zu, mode=upper", rc, timestamp);

    rc = ldb_rollback(db, 9999);
    print_result("rollback to sn=9999 (removed-entries=%zu)", rc, rc);

    rc = ldb_rollback(db, 1010);
    print_result("rollback to sn=1010 (removed-entries=%zu from top)", rc, rc);

    rc = ldb_purge(db, 1003);
    print_result("purge up to sn=1003 (removed-entries=%zu from bottom)", rc, rc);

    rc = ldb_close(db);
    print_result("close", rc, rc);

    // open existing database
    rc = ldb_open(db, "", "example", true);

    printf("\ndatabase content:\n");
    rc = ldb_stats(db, 0, UINT64_MAX, &stats);
    for (size_t sn = stats.min_seqnum; sn <= stats.max_seqnum; sn += MAX_ENTRIES)
    {
        ldb_read(db, sn, rentries, MAX_ENTRIES, &num);
        for (size_t i = 0; i < num; i++)
            print_entry("  ", rentries + i);
    }

    rc = ldb_close(db);

    // free entries used to read
    ldb_free_entry(&rentry);
    ldb_free_entries(rentries, 10);

    return 0;
}

#ifdef LDB_IMPL
int main(void)
{
    ldb_db_t db = {0};

    srand(time(NULL));
    run(&db);

    return 0;
}
#else
int main(void)
{
    ldb_db_t *db = ldb_alloc();

    srand(time(NULL));
    run(db);

    ldb_free(db);
    return 0;
}
#endif
