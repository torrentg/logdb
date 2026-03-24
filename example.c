#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include "journal.h"

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
        .data = (char *) lorem + (rand() % (sizeof(lorem) - 21)),
        .data_len = 20
    };
}

void print_entry(const char *prefix, const ldb_entry_t *entry) {
    printf("%s{ seqnum=%zu, timestamp=%zu, data='%.*s' }\n", 
            prefix,
            entry->seqnum, entry->timestamp, 
            entry->data_len, (char *) entry->data);
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

int run(ldb_journal_t *journal)
{
    ldb_stats_t stats = {0};
    size_t timestamp = 0;
    size_t seqnum1 = 0;
    size_t seqnum2 = 0;
    size_t num = 0;
    int rc = 0;

    size_t buf_len = 128;
    char *buf = malloc(buf_len);
    ldb_entry_t entries[MAX_ENTRIES] = {{0}};
    ldb_entry_t entry = {0};

    // remove existing journal
    remove("example.dat");
    remove("example.idx");

    // create an empty journal
    rc = ldb_open(journal, "", "example", LDB_OPEN_CREATE | LDB_OPEN_CHECK);
    print_result("open", rc);

    rc = ldb_set_meta(journal, "format=1.6", 16);
    print_result("set metadata", rc);

    rc = ldb_get_meta(journal, buf, LDB_METADATA_LEN);
    print_result("get metadata (%s)", rc, buf);

    entry = create_random_entry(1000, 42);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append initial entry (sn=1000 and ts=42)", rc);

    entry = create_random_entry(1001, 42);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with correlative seqnum", rc);

    entry.seqnum = 999;
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with non-correlative seqnum", rc);

    entry.seqnum = 1002;
    entry.timestamp = 40;
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with timestamp less than previous", rc);

    entry = create_random_entry(0, 43);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with seqnum = 0 (assigned next value, %zu)", rc, entry.seqnum);

    entry = create_random_entry(0, 0);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with timestamp = 0 (assigned current millis)", rc);

    // you can enter a batch of entries (1 single flush is done at the end)
    for (size_t i = 0; i < MAX_ENTRIES; i++) {
        entries[i] = create_random_entry(0, 0);
    }
    rc = ldb_append(journal, entries, MAX_ENTRIES, NULL);
    print_result("append 10 entries in a row", rc);

    /// timestamp of last entry
    timestamp = entries[MAX_ENTRIES-1].timestamp;

    rc = ldb_read(journal, 1001, &entry, 1, buf, buf_len, NULL);
    print_result("read existing entry (sn=1001)", rc);

    rc = ldb_read(journal, 9999, &entry, 1, buf, buf_len, NULL);
    print_result("read non-existing entry (sn=9999)", rc);

    // we need to allocate/reallocate the buffer to store the data
    rc = ldb_stats(journal, 1010, 1020, &stats);
    print_result("stats range [1010-1020] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    if (buf_len < stats.data_size)
    {
        char *aux = (char *) realloc(buf, stats.data_size);
        if (aux != NULL) {
            buf_len = stats.data_size;
            buf = aux;
        }
    }

    // you can read multiple entries in a row
    rc = ldb_read(journal, 1010, entries, MAX_ENTRIES, buf, buf_len, &num);
    print_result("read %d entries starting at 1010 (read-entries=%zu)", rc, MAX_ENTRIES, num);

    rc = ldb_stats(journal, 0, 9999, &stats);
    print_result("stats range [0-9999] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_stats(journal, 1005, 1011, &stats);
    print_result("stats range [1005-1011] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_stats(journal, 0, 100, &stats);
    print_result("stats range [0-100] (num-entries=%zu, size=%zu)", rc, stats.num_entries, stats.index_size + stats.data_size);

    rc = ldb_search(journal, 0, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(journal, 0, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=0 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(journal, 42, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(journal, 42, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=42 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(journal, 1000, LDB_SEARCH_LOWER, &seqnum1);
    rc = ldb_search(journal, 1000, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=1000 (lower=%zu, upper=%zu)", rc, seqnum1, seqnum2);

    rc = ldb_search(journal, timestamp, LDB_SEARCH_LOWER, &seqnum1);
    print_result("search ts=%zu, mode=lower", rc, timestamp);

    rc = ldb_search(journal, timestamp, LDB_SEARCH_UPPER, &seqnum2);
    print_result("search ts=%zu, mode=upper", rc, timestamp);

    rc = ldb_rollback(journal, 9999);
    print_result("rollback to sn=9999 (removed-entries=%zu)", rc, rc);

    rc = ldb_rollback(journal, 1010);
    print_result("rollback to sn=1010 (removed-entries=%zu from top)", rc, rc);

    rc = ldb_purge(journal, 1003);
    print_result("purge up to sn=1003 (removed-entries=%zu from bottom)", rc, rc);

    rc = ldb_close(journal);
    print_result("close", rc, rc);

    // open existing journal
    rc = ldb_open(journal, "", "example", LDB_OPEN_CHECK);

    printf("\njournal content:\n");
    rc = ldb_stats(journal, 0, UINT64_MAX, &stats);
    for (size_t sn = stats.min_seqnum; sn <= stats.max_seqnum; sn += MAX_ENTRIES)
    {
        ldb_stats_t tmp = {0};
        rc = ldb_stats(journal, sn, sn + MAX_ENTRIES, &tmp);

        if (buf_len < tmp.data_size)
        {
            char *aux = (char *) realloc(buf, tmp.data_size);
            if (aux != NULL) {
                buf_len = tmp.data_size;
                buf = aux;
            }
        }

        ldb_read(journal, sn, entries, MAX_ENTRIES, buf, buf_len, &num);
        for (size_t i = 0; i < num; i++)
            print_entry("  ", entries + i);
    }

    rc = ldb_close(journal);

    free(buf);

    return 0;
}

int main(void)
{
    ldb_journal_t *journal = ldb_alloc();

    srand(time(NULL));
    run(journal);

    ldb_free(journal);
    return 0;
}
