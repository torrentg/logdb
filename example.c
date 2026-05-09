#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include "journal.h"

#define MAX_ENTRIES 10
#define MIN(a,b) (((a) < (b)) ? (a) : (b))

static const char lorem[] = 
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, " \
    "sed do eiusmod tempor incididunt ut labore et dolore magna " \
    "aliqua. Ut enim ad minim veniam, quis nostrud exercitation " \
    "ullamco laboris nisi ut aliquip ex ea commodo consequat. " \
    "Duis aute irure dolor in reprehenderit in voluptate velit " \
    "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint " \
    "occaecat cupidatat non proident, sunt in culpa qui officia " \
    "deserunt mollit anim id est laborum.";

ldb_entry_t create_random_entry(size_t seqnum) {
    return (ldb_entry_t) {
        .seqnum = seqnum,
        .data = (char *) lorem + (rand() % (sizeof(lorem) - 21)),
        .data_len = 20
    };
}

void print_entry(const char *prefix, const ldb_entry_t *entry) {
    printf("%s{ seqnum=%lu, data='%.*s' }\n", 
            prefix,
            entry->seqnum,
            entry->data_len, 
            (char *) entry->data);
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
    ldb_range_t range = {0};
    ldb_entry_t entries[MAX_ENTRIES] = {{0}};
    ldb_entry_t entry = {0};
    size_t buf_len = 1024;
    char *buf = NULL;
    size_t num = 0;
    int rc = 0;

    // allocate some memory
    buf = malloc(buf_len);

    // remove existing journal
    remove("example.dat");
    remove("example.idx");

    // create an empty journal
    rc = ldb_open(journal, "", "example", LDB_OPEN_CREATE);
    print_result("open", rc);

    rc = ldb_set_meta(journal, "format=1.6", strlen("format=1.6"));
    print_result("set metadata", rc);

    rc = ldb_get_meta(journal, buf, LDB_METADATA_LEN);
    print_result("get metadata (%s)", rc, buf);

    entry = create_random_entry(1000);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append initial entry (seqnum=1000)", rc);

    entry = create_random_entry(1001);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with correlative seqnum", rc);

    entry.seqnum = 999;
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with non-correlative seqnum", rc);

    entry = create_random_entry(0);
    rc = ldb_append(journal, &entry, 1, NULL);
    print_result("append entry with seqnum = 0 (assigned next value, %zu)", rc, entry.seqnum);

    // you can enter a batch of entries (1 single flush is done at the end)
    for (size_t i = 0; i < MAX_ENTRIES; i++) {
        entries[i] = create_random_entry(0);
    }
    rc = ldb_append(journal, entries, MAX_ENTRIES, NULL);
    print_result("append %d entries in a row", rc, MAX_ENTRIES);

    rc = ldb_read(journal, 1001, &entry, 1, buf, buf_len, NULL);
    print_result("read existing entry (seqnum=1001)", rc);

    rc = ldb_read(journal, 9999, &entry, 1, buf, buf_len, NULL);
    print_result("read non-existing entry (seqnum=9999)", rc);

    // you can read multiple entries in a row
    rc = ldb_read(journal, 1010, entries, MAX_ENTRIES, buf, buf_len, &num);
    print_result("read %d entries starting at 1010 (read-entries=%zu)", rc, MAX_ENTRIES, num);

    range = ldb_get_range(journal);
    print_result("get range = [%lu,%lu]", 0, range.min_seqnum, range.max_seqnum);

    rc = ldb_rollback(journal, 9999);
    print_result("rollback to seqnum=9999 (removed-entries=%zu)", rc, rc);

    rc = ldb_rollback(journal, 1010);
    print_result("rollback to seqnum=1010 (removed-entries=%zu from top)", rc, rc);

    rc = ldb_purge(journal, 1003);
    print_result("purge up to seqnum=1003 (removed-entries=%zu from bottom)", rc, rc);

    rc = ldb_close(journal);
    print_result("close journal", rc);

    // open existing journal
    rc = ldb_open(journal, "", "example", 0);
    print_result("open existing journal", rc);

    printf("\njournal content:\n");
    range = ldb_get_range(journal);

    if (range.min_seqnum == 0)
    {
        printf("  (no entries)\n");
    }
    else
    {
        uint64_t seq = range.min_seqnum;
        uint64_t to_seq = range.max_seqnum;

        while (seq <= to_seq)
        {
            size_t want = MIN(MAX_ENTRIES, to_seq - seq + 1);

            // read entries in batches
            rc = ldb_read(journal, seq, entries, want, buf, buf_len, &num);

            if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND) {
                printf("\nError reading journal: %s\n", ldb_strerror(rc));
                break;
            }

            for (size_t i = 0; i < num; i++)
                print_entry("  ", entries + i);

            if (num < want)
            {
                // case: reached end of this journal
                if (entries[num].seqnum == 0)
                    break;

                // case: buffer too short
                char *ptr = NULL;
                size_t need = (size_t) entries[num].data_len + 32;

                while (buf_len < need)
                    buf_len *= 2;

                if ((ptr = (char *) realloc(buf, buf_len)) == NULL) {
                    printf("\nError resizing buffer: out of memory\n");
                    break;
                }

                buf = ptr;
            }

            // set next seqnum to read
            if (num > 0)
                seq = entries[num - 1].seqnum + 1;
        }
    }

    ldb_close(journal);

    free(buf);

    return 0;
}

int main(void)
{
    ldb_journal_t *journal = ldb_alloc();

    run(journal);

    ldb_free(journal);
    return 0;
}
