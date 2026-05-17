#include "journal.h"
#include "journal.c"
#include "acutest.h"

// ===========================================
// Helpers
// ===========================================

void append_entries(ldb_journal_t *journal, uint64_t seqnum1, uint64_t seqnum2)
{
    char data[128] = {0};

    if (seqnum2 < seqnum1)
        seqnum2 = seqnum1;

    while (seqnum1 <= seqnum2)
    {
        snprintf(data, sizeof(data), "data-%d", (int)seqnum1);

        ldb_entry_t entry = {
            .seqnum = seqnum1,
            .data_len = (uint32_t)strlen(data) + 1,
            .data = data
        };

        TEST_ASSERT(ldb_append(journal, &entry, 1, NULL) == LDB_OK);

        seqnum1++;
    }
}

bool check_entry(const ldb_entry_t *entry, uint64_t seqnum, const char *data)
{
    return (entry &&
            entry->seqnum == seqnum &&
            entry->data_len == (data == NULL ? 0 : strlen(data) + 1) &&
            (entry->data == data || (entry->data != NULL && data != NULL && strcmp(entry->data, data) == 0)));
}

// ===========================================
// Tests
// ===========================================

void test_version(void)
{
    const char *version = ldb_version();
    TEST_CHECK(version != NULL);

    size_t len = strlen(version);
    TEST_ASSERT(len >= 5);

    TEST_CHECK(version[0] != '.');
    TEST_CHECK(version[len-1] != '.');

    size_t num_dots = 0;
    for (size_t i = 0; i < len; i++) {
        if (version[i] == '.')
            num_dots++;
        else if (!isdigit(version[i]))
            TEST_CHECK(false);
    }

    TEST_CHECK(num_dots == 2);
}

void test_strerror(void)
{
    const char *success = ldb_strerror(LDB_OK);

    TEST_CHECK(ldb_strerror(0) == ldb_strerror(LDB_OK));
    TEST_CHECK(strcmp(ldb_strerror(0), success) == 0);

    const char *unknown_error = ldb_strerror(-999);
    TEST_CHECK(unknown_error != NULL);

    for (int i = 0; i < 26; i++) {
        TEST_CHECK(ldb_strerror(-i) != NULL);
        TEST_CHECK(strcmp(ldb_strerror(-i), unknown_error) != 0);
    }
    for (int i = 26; i < 32; i++) {
        TEST_CHECK(ldb_strerror(-i) != NULL);
        TEST_CHECK(strcmp(ldb_strerror(-i), unknown_error) == 0);
    }
    for (int i = 1; i < 32; i++) {
        TEST_CHECK(ldb_strerror(i) != NULL);
        TEST_CHECK(strcmp(ldb_strerror(i), success) == 0);
    }
}

void test_sizeof(void)
{
    TEST_CHECK(sizeof(ldb_header_dat_t) % sizeof(uintptr_t) == 0);
    TEST_CHECK(sizeof(ldb_record_dat_t) == 16);
    TEST_CHECK(sizeof(ldb_header_idx_t) % sizeof(uintptr_t) == 0);
    TEST_CHECK(sizeof(ldb_record_idx_t) == 8);
    TEST_CHECK(sizeof(ldb_range_t) == 16);
    TEST_CHECK(sizeof(ldb_entry_t) == 24);
}

// Results validated using https://crccalc.com/
void test_crc32(void)
{
    // abnormal cases
    TEST_CHECK(ldb_crc32(NULL, 0, 42) == 42);
    TEST_CHECK(ldb_crc32(NULL, 10, 42) == 42);
    TEST_CHECK(ldb_crc32("", 0, 42) == 42);

    // basic case
    const char str1[] = "hello world";
    TEST_CHECK(ldb_crc32(str1, strlen(str1), 0) == 0x0D4A1185);

    // composability
    const char str11[] = "hello ";
    const char str12[] = "world";
    size_t checksum = ldb_crc32(str11, strlen(str11), 0);
    checksum = ldb_crc32(str12, strlen(str12), checksum);
    TEST_CHECK(checksum == 0x0D4A1185);
}

void test_is_valid_path(void)
{
    TEST_CHECK(ldb_is_valid_path(""));
    TEST_CHECK(ldb_is_valid_path("."));
    TEST_CHECK(ldb_is_valid_path("./"));
    TEST_CHECK(ldb_is_valid_path("/tmp"));
    TEST_CHECK(ldb_is_valid_path("/tmp/"));
    TEST_CHECK(ldb_is_valid_path("//tmp"));

    TEST_CHECK(!ldb_is_valid_path(NULL));
    TEST_CHECK(!ldb_is_valid_path("/non_existent_dir/"));
    TEST_CHECK(!ldb_is_valid_path("/etc/passwd"));
}

void test_is_valid_name(void)
{
    TEST_CHECK(ldb_is_valid_name("test"));
    TEST_CHECK(ldb_is_valid_name("test_1"));
    TEST_CHECK(ldb_is_valid_name("_"));
    TEST_CHECK(ldb_is_valid_name("a"));
    TEST_CHECK(ldb_is_valid_name("abc"));
    TEST_CHECK(ldb_is_valid_name("test-a"));
    TEST_CHECK(ldb_is_valid_name("test-b"));
    TEST_CHECK(ldb_is_valid_name("my-journal-1"));
    TEST_CHECK(ldb_is_valid_name("-leading-hyphen"));

    TEST_CHECK(!ldb_is_valid_name(NULL));
    TEST_CHECK(!ldb_is_valid_name(""));
    TEST_CHECK(!ldb_is_valid_name("too_long_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));

    char name[] = "x";
    for (int i = 1; i < 256; i++) {
        name[0] = (char)i;
        TEST_CHECK(ldb_is_valid_name(name) == (isalnum(name[0]) || strchr("_-", name[0]) != NULL));
    }
}

void test_create_filename(void)
{
    char *filename = NULL;

    TEST_CHECK(ldb_create_filename(NULL, "name", ".ext") == NULL);
    TEST_CHECK(ldb_create_filename("path", NULL, ".ext") == NULL);
    TEST_CHECK(ldb_create_filename("path", "name", NULL) == NULL);
    TEST_CHECK(ldb_create_filename("path", "",   ".ext") == NULL);

    filename = ldb_create_filename("path", "name", ".ext");
    TEST_CHECK(filename != NULL && strcmp(filename, "path/name.ext") == 0);
    free(filename);

    filename = ldb_create_filename("path/", "name", ".ext");
    TEST_CHECK(filename != NULL && strcmp(filename, "path/name.ext") == 0);
    free(filename);

    filename = ldb_create_filename("", "name", ".ext");
    TEST_CHECK(filename != NULL && strcmp(filename, "name.ext") == 0);
    free(filename);
}

void test_close(void)
{
    ldb_journal_t journal = {0};

    TEST_CHECK(ldb_close(NULL) == LDB_OK);
    TEST_CHECK(ldb_close(&journal) == LDB_OK);

    journal.name = (char *)malloc(10);
    journal.path = (char *)malloc(10);
    journal.dat_path = (char *)malloc(10);
    journal.idx_path = (char *)malloc(10);
    journal.dat_fp = NULL;
    journal.idx_fp = NULL;

    TEST_CHECK(ldb_close(&journal) == LDB_OK);
    TEST_CHECK(journal.name == NULL);
    TEST_CHECK(journal.path == NULL);
    TEST_CHECK(journal.dat_path == NULL);
    TEST_CHECK(journal.idx_path == NULL);
    TEST_CHECK(journal.dat_fp == NULL);
    TEST_CHECK(journal.idx_fp == NULL);
}

void test_open_invalid_args(void) {
    TEST_CHECK(ldb_open(NULL, "/tmp/", "test", 0) == LDB_ERR_ARG);  // no object
}

void test_open_invalid_path(void) {
    ldb_journal_t journal = {0};
    TEST_CHECK(ldb_open(&journal, NULL   , "test", 0) == LDB_ERR_PATH);                 // no path
    TEST_CHECK(ldb_open(&journal, "/non_existent_path/", "test", 0) == LDB_ERR_PATH);   // non-existent dir
    TEST_CHECK(ldb_open(&journal, "/etc/passwd", "test", 0) == LDB_ERR_PATH);           // file instead of dir
}

void test_open_invalid_name(void) {
    ldb_journal_t journal = {0};
    TEST_CHECK(ldb_open(&journal, "/tmp/",  NULL , 0) == LDB_ERR_NAME);
    TEST_CHECK(ldb_open(&journal, "/tmp/", "", 0) == LDB_ERR_NAME);
    TEST_CHECK(ldb_open(&journal, "/tmp/", ".", 0) == LDB_ERR_NAME);
    TEST_CHECK(ldb_open(&journal, "/tmp/", "xxx?", 0) == LDB_ERR_NAME);
    TEST_CHECK(ldb_open(&journal, "/tmp/", "too_long_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 0) == LDB_ERR_NAME);
}

void test_open_create(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(journal.name != NULL && strcmp(journal.name, "test") == 0);
    TEST_CHECK(journal.path != NULL && strcmp(journal.path, "") == 0);
    TEST_CHECK(journal.dat_path != NULL && strcmp(journal.dat_path, "test.dat") == 0);
    TEST_CHECK(journal.idx_path != NULL && strcmp(journal.idx_path, "test.idx") == 0);
    TEST_CHECK(journal.dat_fp != NULL);
    TEST_CHECK(journal.idx_fp != NULL);
    TEST_CHECK(journal.state.min_seqnum == 0);
    TEST_CHECK(journal.state.max_seqnum == 0);
    TEST_CHECK(journal.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&journal);
}

void test_open_empty(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // create journal
    ldb_create_dat("test.dat");

    // open empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(journal.name != NULL && strcmp(journal.name, "test") == 0);
    TEST_CHECK(journal.path != NULL && strcmp(journal.path, "") == 0);
    TEST_CHECK(journal.dat_path != NULL && strcmp(journal.dat_path, "test.dat") == 0);
    TEST_CHECK(journal.idx_path != NULL && strcmp(journal.idx_path, "test.idx") == 0);
    TEST_CHECK(journal.dat_fp != NULL);
    TEST_CHECK(journal.idx_fp != NULL);
    TEST_CHECK(journal.state.min_seqnum == 0);
    TEST_CHECK(journal.state.max_seqnum == 0);
    TEST_CHECK(journal.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&journal);
}

void test_open_invl_dat_header(void)
{
    FILE *fp = NULL;
    ldb_journal_t journal = {0};
    ldb_header_dat_t header = {
        .magic_number = LDB_DAT_MAGIC_NUMBER,
        .format = LDB_FILE_FORMAT,
        .padding = 0,
        .metadata = {0}
    };

    remove("test.dat");
    remove("test.idx");

    // empty file
    fp = fopen("test.dat", "w");
    fclose(fp);
    TEST_CHECK(ldb_open(&journal, "", "test", 0) == LDB_ERR_INVL_DAT);

    // invalid magic numer
    fp = fopen("test.dat", "w");
    header.magic_number = 123;
    fwrite(&header, sizeof(ldb_header_dat_t), 1, fp);
    fclose(fp);
    TEST_CHECK(ldb_open(&journal, "", "test", 0) == LDB_ERR_INVL_DAT);

    // invalid file format
    fp = fopen("test.dat", "w");
    header.magic_number = LDB_DAT_MAGIC_NUMBER;
    header.format = LDB_FILE_FORMAT + 1;
    fwrite(&header, sizeof(ldb_header_dat_t), 1, fp);
    fclose(fp);
    TEST_CHECK(ldb_open(&journal, "", "test", 0) == LDB_ERR_INVL_DAT);
}

void test_open_and_repair_1(void)
{
    ldb_journal_t journal = {0};
    ldb_record_dat_t record = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // writing invalid data (first record too short)
    const char garbage[] = "ioscm,nswddljkh";
    fwrite(garbage, sizeof(garbage), 1, journal.dat_fp);
    ldb_close(&journal);

    // incomplete record is zeroized
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // writing invalid first record
    record.seqnum = 1;
    record.data_len = 1000;    // has data length but data not added after record
    fwrite(&record, sizeof(ldb_record_dat_t), 1, journal.dat_fp);
    ldb_close(&journal);

    // first entry zeroized
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 0);
    ldb_close(&journal);
}

void test_open_and_repair_2(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting 1 valid entry
    const char data[32000] = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .data_len = 21640,
        .data = (char *)data
    };
    TEST_ASSERT(ldb_append(&journal, &entry, 1, NULL) == LDB_OK);

    // inserting a partially zeroized entry
    ldb_record_dat_t record = {
        .seqnum = 0,
        .data_len = 400,
    };
    fwrite(&record, sizeof(ldb_record_dat_t), 1, journal.dat_fp);

    // inserting garbage
    const char garbage[] = "ioscm,nswddljk";
    fwrite(&garbage, sizeof(garbage), 1, journal.dat_fp);
    ldb_close(&journal);

    // incomplete record is zeroized
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.max_seqnum == 10);

    ldb_close(&journal);
}

void test_open_and_repair_3(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting 1 valid entry
    const char data[1024] = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .data_len = 400,
        .data = (char *)data
    };
    TEST_ASSERT(ldb_append(&journal, &entry, 1, NULL) == LDB_OK);

    // inserting 1 'valid' entry with invalid data length
    ldb_record_dat_t record = {
        .seqnum = entry.seqnum + 1,
        .data_len = 400,
        .checksum = 999       // invalid but checked after data length
    };
    fwrite(&record, sizeof(ldb_record_dat_t), 1, journal.dat_fp);
    fwrite(data, record.data_len - 10, 1, journal.dat_fp);
    ldb_close(&journal);

    // second record (incomplete) is zeroized
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.max_seqnum == 10);

    ldb_close(&journal);
}

void test_open_1_entry_ok(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting 1 entry
    const char data[] = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    ldb_entry_t entry = {
        .seqnum = 10,
        .data_len = (uint32_t)strlen(data),
        .data = (char *)data
    };
    TEST_ASSERT(ldb_append(&journal, &entry, 1, NULL) == LDB_OK);
    ldb_close(&journal);

    // open journal with 1-entry (idx will be rebuild)
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 10);
    TEST_CHECK(journal.dat_end == sizeof(ldb_header_dat_t) + sizeof(ldb_record_dat_t) + entry.data_len + ldb_padding(entry.data_len));
    ldb_close(&journal);

    // open journal with 1-entry (idx no rebuilded)
    TEST_CHECK(ldb_open(&journal, "", "test", 0) == LDB_OK);
    ldb_close(&journal);
}

void test_open_1_entry_empty(void)
{
    const char data[128] = {0};
    ldb_journal_t journal = {0};
    ldb_record_dat_t record = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting 1 entry (empty)
    fwrite(&record, sizeof(ldb_record_dat_t), 1, journal.dat_fp);

    // inserting additional empty content
    fwrite(data, sizeof(data), 1, journal.dat_fp);
    ldb_close(&journal);

    // open journal with 1-entry (idx will be rebuild)
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 0);
    TEST_CHECK(journal.state.max_seqnum == 0);
    TEST_CHECK(journal.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&journal);
}

void test_open_dat_corrupted(void)
{
    const char data[1024] = {0};
    ldb_journal_t journal = {0};
    ldb_record_dat_t record_dat = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting entry-1
    record_dat.seqnum = 10;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.data_len, checksum);
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, journal.dat_fp);
    fwrite(data, record_dat.data_len, 1, journal.dat_fp);
    fwrite(data, ldb_padding(record_dat.data_len), 1, journal.dat_fp);

    // inserting entry-2 (incorrect checksum)
    record_dat.seqnum = 11;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.data_len, checksum) + 999;
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, journal.dat_fp);
    fwrite(data, record_dat.data_len, 1, journal.dat_fp);
    fwrite(data, ldb_padding(record_dat.data_len), 1, journal.dat_fp);

    ldb_close(&journal);

    // open journal (identify that last record is corrupted, remove it and rebuilds index)
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    ldb_close(&journal);
}

void test_open_idx_corrupted(void)
{
    ldb_journal_t journal = {0};
    ldb_record_idx_t record_idx = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // inserting entries
    append_entries(&journal, 10, 14);

    // append an invalid index entry
    record_idx.pos = 5000;
    fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, journal.idx_fp);

    ldb_close(&journal);

    // open identify error in last idx entry and rebuilds index
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 14);
    ldb_close(&journal);

    // Caution:
    // ldb_open() validates first and last idx records but not the ones in between.
    // Use ldb_check() to validate intermediate records.
}

void test_open_idx_missing_last_entry(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entries[3] = {{0}};
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create journal with 5 entries (seqnum 10..14)
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 14);
    ldb_close(&journal);

    // truncate idx to remove last entry (simulate dat flushed but idx not)
    FILE *fp = fopen("test.idx", "r+b");
    TEST_ASSERT(fp != NULL);
    fseek(fp, 0, SEEK_END);
    long idx_size = ftell(fp);
    TEST_ASSERT(idx_size > (long)sizeof(ldb_record_idx_t));
    ftruncate(fileno(fp), idx_size - (long)sizeof(ldb_record_idx_t));
    fclose(fp);

    // open journal (should detect unindexed entry and rebuild idx)
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 14);

    // verify last entry is readable
    TEST_CHECK(ldb_read(&journal, 14, entries, 1, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 1);
    TEST_CHECK(check_entry(&entries[0], 14, "data-14"));

    // verify all entries are readable
    TEST_CHECK(ldb_read(&journal, 10, entries, 3, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 3);
    TEST_CHECK(check_entry(&entries[0], 10, "data-10"));
    TEST_CHECK(check_entry(&entries[1], 11, "data-11"));
    TEST_CHECK(check_entry(&entries[2], 12, "data-12"));

    ldb_close(&journal);
}

void test_append_invalid_args(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entry = {0};

    TEST_CHECK(ldb_append(NULL, &entry, 1, NULL) == LDB_ERR_ARG);    // No journal
    TEST_CHECK(ldb_append(&journal, NULL, 1, NULL) == LDB_ERR_ARG);  // No entries
    TEST_CHECK(ldb_append(&journal, &entry, 1, NULL) == LDB_ERR);    // Journal not open
}

void test_append_nothing(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entries[10];
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // append 0 entries
    TEST_CHECK(ldb_append(&journal, entries, 0, &num) == LDB_OK);
    TEST_CHECK(num == 0);

    ldb_close(&journal);
}

void test_append_auto(void)
{
    ldb_journal_t journal = {0};
    const size_t len = 3;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
        snprintf(buf, sizeof(buf), "data-%d", (int)i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    // append 3 entries
    TEST_ASSERT(ldb_append(&journal, entries, len, &num) == LDB_OK);
    TEST_CHECK(num == len);
    TEST_CHECK(journal.state.min_seqnum == 1);
    TEST_CHECK(journal.state.max_seqnum == len);
    TEST_CHECK(entries[0].seqnum == 1);
    TEST_CHECK(entries[1].seqnum == 2);
    TEST_CHECK(entries[2].seqnum == 3);

    // append 3 entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
    }

    TEST_ASSERT(ldb_append(&journal, entries, len, &num) == LDB_OK);
    TEST_CHECK(num == len);
    TEST_CHECK(journal.state.min_seqnum == 1);
    TEST_CHECK(journal.state.max_seqnum == 2*len);
    TEST_CHECK(entries[0].seqnum == 4);
    TEST_CHECK(entries[1].seqnum == 5);
    TEST_CHECK(entries[2].seqnum == 6);

    ldb_close(&journal);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++)
        free(entries[i].data);
}

void test_append_nominal_case(void)
{
    ldb_journal_t journal = {0};
    const size_t len = 10;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 10 + i;
        snprintf(buf, sizeof(buf), "data-%d", (int)i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    TEST_ASSERT(ldb_append(&journal, entries, len, &num) == LDB_OK);
    TEST_CHECK(num == len);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 10 + len - 1);

    ldb_close(&journal);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++)
        free(entries[i].data);
}

void test_append_broken_sequence(void)
{
    ldb_journal_t journal = {0};
    const size_t len = 10;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 10 + i + (i == 5 ? 40 : 0);
        snprintf(buf, sizeof(buf), "data-%d", (int)i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    TEST_ASSERT(ldb_append(&journal, entries, len, &num) == LDB_ERR_SEQNUM);
    TEST_CHECK(num == 5);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 10 + num - 1);

    ldb_close(&journal);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++)
        free(entries[i].data);
}

void test_append_lack_of_data(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .data_len = 40,
        .data = NULL
    };

    remove("test.dat");
    remove("test.idx");

    // create empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    TEST_CHECK(ldb_append(&journal, &entry, 1, NULL) == LDB_ERR_NODATA);

    ldb_close(&journal);
}

void test_read_invalid_args(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entries[3] = {{0}};
    char buf[1024] = {0};

    TEST_CHECK(ldb_read(NULL, 1, entries, 3, buf, sizeof(buf), NULL) == LDB_ERR_ARG);      // NULL journal
    TEST_CHECK(ldb_read(&journal, 1, NULL, 3, buf, sizeof(buf), NULL) == LDB_ERR_ARG);     // NULL entries
    TEST_CHECK(ldb_read(&journal, 1, entries, 0, buf, sizeof(buf), NULL) == LDB_ERR_ARG);  // length(entries) = 0
    TEST_CHECK(ldb_read(&journal, 1, entries, 3, NULL, sizeof(buf), NULL) == LDB_ERR_ARG); // NULL buffer
    TEST_CHECK(ldb_read(&journal, 1, entries, 3, buf, 3, NULL) == LDB_ERR_ARG);            // length(buf) < 24
    TEST_CHECK(ldb_read(&journal, 1, entries, 3, buf, sizeof(buf), NULL) == LDB_ERR);      // journal not open
}

void test_read_empty(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entries[3] = {{0}};
    char buf[1024] = {0};
    size_t num = 10;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    entries[0].seqnum = 5;
    entries[1].seqnum = 15;
    entries[2].seqnum = 25;

    TEST_CHECK(ldb_read(&journal, 0, entries, 3, buf, sizeof(buf), &num) == LDB_ERR_NOT_FOUND);
    TEST_CHECK(num == 0);

    TEST_CHECK(ldb_read(&journal, 2, entries, 3, buf, sizeof(buf), &num) == LDB_ERR_NOT_FOUND);
    TEST_CHECK(num == 0);

    TEST_CHECK(entries[0].seqnum == 0);
    TEST_CHECK(entries[1].seqnum == 0);
    TEST_CHECK(entries[2].seqnum == 0);

    ldb_close(&journal);
}

void test_read_nominal_case(void)
{
    ldb_journal_t journal = {0};
    ldb_entry_t entries[10] = {{0}};
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 5, 314);

    TEST_CHECK(ldb_read(&journal, 0, entries, 3, buf, sizeof(buf), &num) == LDB_ERR_NOT_FOUND);
    TEST_CHECK(num == 0);

    TEST_CHECK(ldb_read(&journal, 3, entries, 3, buf, sizeof(buf), &num) == LDB_ERR_NOT_FOUND);
    TEST_CHECK(num == 0);

    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 3);
    TEST_CHECK(check_entry(&entries[0], 20, "data-20"));
    TEST_CHECK(check_entry(&entries[1], 21, "data-21"));
    TEST_CHECK(check_entry(&entries[2], 22, "data-22"));

    TEST_CHECK(ldb_read(&journal, 40, entries, 2, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 2);
    TEST_CHECK(check_entry(&entries[0], 40, "data-40"));
    TEST_CHECK(check_entry(&entries[1], 41, "data-41"));

    TEST_CHECK(ldb_read(&journal, 313, entries, 3, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 2);
    TEST_CHECK(check_entry(&entries[0], 313, "data-313"));
    TEST_CHECK(check_entry(&entries[1], 314, "data-314"));

    TEST_CHECK(ldb_read(&journal, 400, entries, 3, buf, sizeof(buf), &num) == LDB_ERR_NOT_FOUND);
    TEST_CHECK(num == 0);

    // entry1 = 16 + 8 = 24, entry2 = 16 + 8 = 24

    // case0: buffer too small (read truncates 1st entry header)
    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, 15, &num) == LDB_ERR_ARG);

    // case1: buffer too small (read truncates 1st entry data)
    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, 20, &num) == LDB_OK);
    TEST_CHECK(num == 0);
    TEST_CHECK(entries[0].seqnum == 20);
    TEST_CHECK(entries[0].data_len == 8);
    TEST_CHECK(entries[0].data == NULL);
    TEST_CHECK(entries[1].seqnum == 0);

    // case2: buffer too small (read truncates 2nd entry header)
    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, 26, &num) == LDB_OK);
    TEST_CHECK(num == 0);
    TEST_CHECK(entries[0].seqnum == 20);
    TEST_CHECK(entries[0].data_len == 8);
    TEST_CHECK(entries[0].data == NULL);
    TEST_CHECK(entries[1].seqnum == 0);

    // case3: buffer too small (read truncates 2nd entry data)
    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, 46, &num) == LDB_OK);
    TEST_CHECK(num == 1);
    TEST_CHECK(check_entry(&entries[0], 20, "data-20"));
    TEST_CHECK(entries[1].seqnum == 21);
    TEST_CHECK(entries[1].data_len == 8);
    TEST_CHECK(entries[1].data == NULL);
    TEST_CHECK(entries[2].seqnum == 0);

    // case4: buffer just enough for 3 entries
    TEST_CHECK(ldb_read(&journal, 20, entries, 3, buf, 72, &num) == LDB_OK);
    TEST_CHECK(num == 3);
    TEST_CHECK(check_entry(&entries[0], 20, "data-20"));
    TEST_CHECK(check_entry(&entries[1], 21, "data-21"));
    TEST_CHECK(check_entry(&entries[2], 22, "data-22"));

    ldb_close(&journal);
}

void test_range_all(void)
{
    ldb_journal_t journal = {0};
    ldb_range_t range = {0};

    remove("test.dat");
    remove("test.idx");

    // non-valid journal
    range = ldb_get_range(NULL);
    TEST_CHECK(range.min_seqnum == UINT64_MAX);
    TEST_CHECK(range.max_seqnum == UINT64_MAX);

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // empty journal
    range = ldb_get_range(&journal);
    TEST_CHECK(range.min_seqnum == 0);
    TEST_CHECK(range.max_seqnum == 0);

    append_entries(&journal, 20, 314);

    // non-empty journal
    range = ldb_get_range(&journal);
    TEST_CHECK(range.min_seqnum == 20);
    TEST_CHECK(range.max_seqnum == 314);

    ldb_close(&journal);
}

void test_rollback_invalid_args(void)
{
    ldb_journal_t journal = {0};

    TEST_CHECK(ldb_rollback(NULL, 1) == LDB_ERR_ARG);   // NULL journal
    TEST_CHECK(ldb_rollback(&journal, 1) == LDB_ERR);   // journal not open
}

void test_rollback_nominal_case(void)
{
    ldb_journal_t journal = {0};
    size_t end = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    TEST_CHECK(ldb_rollback(&journal, 0) == 0);
    TEST_CHECK(ldb_rollback(&journal, 1) == 0);

    append_entries(&journal, 20, 314);
    end = journal.dat_end;

    TEST_CHECK(ldb_rollback(&journal, 400) == 0);
    TEST_CHECK(journal.state.min_seqnum == 20);
    TEST_CHECK(journal.state.max_seqnum == 314);
    TEST_CHECK(journal.dat_end == end);

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    TEST_CHECK(ldb_rollback(&journal, 314) == 0);
    TEST_CHECK(journal.state.min_seqnum == 20);
    TEST_CHECK(journal.state.max_seqnum == 314);
    TEST_CHECK(journal.dat_end == end);

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    TEST_ASSERT(ldb_rollback(&journal, 313) == 1);
    TEST_ASSERT(journal.state.min_seqnum == 20);
    TEST_ASSERT(journal.state.max_seqnum == 313);
    TEST_ASSERT(journal.dat_end < end);
    end = journal.dat_end;

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    TEST_CHECK(ldb_rollback(&journal, 100) == 213);
    TEST_CHECK(journal.state.min_seqnum == 20);
    TEST_CHECK(journal.state.max_seqnum == 100);
    TEST_CHECK(journal.dat_end < end);
    end = journal.dat_end;

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    TEST_CHECK(ldb_rollback(&journal, 20) == 80);
    TEST_CHECK(journal.state.min_seqnum == 20);
    TEST_CHECK(journal.state.max_seqnum == 20);
    TEST_CHECK(journal.dat_end < end);
    end = journal.dat_end;

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    TEST_CHECK(ldb_rollback(&journal, 0) == 1);
    TEST_CHECK(journal.state.min_seqnum == 0);
    TEST_CHECK(journal.state.max_seqnum == 0);
    TEST_CHECK(journal.dat_end < end);

    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);

    ldb_close(&journal);
}

void test_alloc_all(void)
{
    ldb_journal_t *journal = ldb_alloc();
    TEST_CHECK(journal != NULL);
    ldb_free(journal);
    ldb_free(NULL);
}

void test_fsync_all(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE | LDB_OPEN_FSYNC) == LDB_OK);
    ldb_close(&journal);

    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_readonly_open(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // cannot open non-existent journal in read-only (no CREATE)
    TEST_CHECK(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_ERR_NOFILE_DAT);

    // open unexistent journal with CREATE + READONLY (should create empty journal and open in read-only)
    TEST_CHECK(ldb_open(&journal, "", "test", LDB_OPEN_CREATE | LDB_OPEN_READONLY) == LDB_OK);
    ldb_close(&journal);

    // create a journal with some data
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    ldb_entry_t entry = { .seqnum = 1, .data_len = 0, .data = NULL };
    TEST_ASSERT(ldb_append(&journal, &entry, 1, NULL) == LDB_OK);
    ldb_close(&journal);

    // open in read-only mode
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_OK);
    TEST_CHECK(journal.read_only == true);
    TEST_CHECK(journal.state.min_seqnum == 1);
    TEST_CHECK(journal.state.max_seqnum == 1);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_readonly_write_ops(void)
{
    ldb_journal_t journal = {0};
    char buf[256] = {0};
    char meta[LDB_METADATA_LEN] = {0};

    remove("test.dat");
    remove("test.idx");

    // create a journal with data
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    ldb_entry_t entries[3] = {
        { .seqnum = 1, .data_len = 0, .data = NULL },
        { .seqnum = 2, .data_len = 0, .data = NULL },
        { .seqnum = 3, .data_len = 0, .data = NULL },
    };
    TEST_ASSERT(ldb_append(&journal, entries, 3, NULL) == LDB_OK);
    ldb_close(&journal);

    // open read-only and try all write operations
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_OK);

    ldb_entry_t new_entry = { .seqnum = 4, .data_len = 0, .data = NULL };
    TEST_CHECK(ldb_append(&journal, &new_entry, 1, NULL) == LDB_ERR_READONLY);
    TEST_CHECK(ldb_rollback(&journal, 2) == (long)LDB_ERR_READONLY);
    TEST_CHECK(ldb_set_meta(&journal, buf, 5) == LDB_ERR_READONLY);

    // verify state was not modified
    TEST_CHECK(journal.state.min_seqnum == 1);
    TEST_CHECK(journal.state.max_seqnum == 3);

    // read operation must work
    ldb_entry_t read_entries[3];
    size_t num = 0;
    TEST_CHECK(ldb_read(&journal, 1, read_entries, 3, buf, sizeof(buf), &num) == LDB_OK);
    TEST_CHECK(num == 3);

    // get_range operation must work
    ldb_range_t range = ldb_get_range(&journal);
    TEST_CHECK(range.min_seqnum == 1);
    TEST_CHECK(range.max_seqnum == 3);

    // read meta must work
    TEST_CHECK(ldb_get_meta(&journal, meta, LDB_METADATA_LEN) == LDB_OK);

    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_readonly_no_flock(void)
{
    ldb_journal_t journal1 = {0};
    ldb_journal_t journal2 = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal1, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // a second read-only open must succeed even while journal1 holds the exclusive lock
    TEST_CHECK(ldb_open(&journal2, "", "test", LDB_OPEN_READONLY) == LDB_OK);

    ldb_close(&journal1);
    ldb_close(&journal2);

    remove("test.dat");
    remove("test.idx");
}

void test_readonly_missing_idx(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // create a journal with data
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    ldb_entry_t entry = { .seqnum = 1, .data_len = 0, .data = NULL };
    TEST_ASSERT(ldb_append(&journal, &entry, 1, NULL) == LDB_OK);
    ldb_close(&journal);

    // remove the index file: read-only mode cannot rebuild it
    remove("test.idx");
    TEST_CHECK(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_ERR_NOFILE_IDX);

    remove("test.dat");
    remove("test.idx");
}

void test_flock(void)
{
    ldb_journal_t journal1 = {0};
    ldb_journal_t journal2 = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal1, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(ldb_open(&journal2, "", "test", 0) == LDB_ERR_LOCK);
    ldb_close(&journal1);
}

void test_meta_all(void)
{
    char buf[2 * LDB_METADATA_LEN] = {0};
    char metadata[LDB_METADATA_LEN] = {0};
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_CHECK(ldb_get_meta(NULL, buf, 10) == LDB_ERR_ARG);                         // NULL journal
    TEST_CHECK(ldb_get_meta(&journal, NULL, 10) == LDB_ERR_ARG);                    // NULL buffer
    TEST_CHECK(ldb_get_meta(&journal, buf, LDB_METADATA_LEN + 1) == LDB_ERR_ARG);   // length(buf) exceeds limit
    TEST_CHECK(ldb_get_meta(&journal, buf, LDB_METADATA_LEN - 1) == LDB_ERR);       // uninitialized journal

    TEST_CHECK(ldb_set_meta(NULL, buf, 10) == LDB_ERR_ARG);                         // NULL journal
    TEST_CHECK(ldb_set_meta(&journal, NULL, 10) == LDB_ERR_ARG);                    // NULL buffer
    TEST_CHECK(ldb_set_meta(&journal, buf, LDB_METADATA_LEN + 1) == LDB_ERR_ARG);   // length(buf) exceeds limit
    TEST_CHECK(ldb_set_meta(&journal, buf, LDB_METADATA_LEN - 1) == LDB_ERR);       // uninitialized journal

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);

    // metadata default value is 0
    TEST_CHECK(ldb_get_meta(&journal, metadata, LDB_METADATA_LEN) == LDB_OK);
    TEST_CHECK(memcmp(metadata, buf, LDB_METADATA_LEN) == 0);

    sprintf(buf, "hello world");
    TEST_CHECK(ldb_set_meta(&journal, buf, strlen(buf) + 1) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&journal, metadata, strlen(buf) + 1) == LDB_OK);
    TEST_CHECK(memcmp(metadata, buf, strlen(buf) + 1) == 0);

    memset(buf, 0x0, sizeof(buf));
    sprintf(buf, "hello");
    TEST_CHECK(ldb_set_meta(&journal, buf, strlen(buf) + 1) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&journal, metadata, LDB_METADATA_LEN) == LDB_OK);
    TEST_CHECK(memcmp(metadata, buf, strlen(buf) + 1) == 0);

    memset(buf, 0x0, sizeof(buf));
    TEST_CHECK(ldb_set_meta(&journal, buf, 0) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&journal, metadata, LDB_METADATA_LEN) == LDB_OK);
    TEST_CHECK(memcmp(metadata, buf, LDB_METADATA_LEN) == 0);

    ldb_close(&journal);
}

void test_check_invalid_args(void)
{
    TEST_CHECK(ldb_check(NULL, false) == LDB_ERR_ARG);
}

void test_check_valid_journal(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // empty journal
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true) == LDB_OK);
    ldb_close(&journal);

    // journal with entries
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    append_entries(&journal, 10, 20);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true) == LDB_OK);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_check_dat_trailing_data(void)
{
    ldb_journal_t journal = {0};
    const char garbage[] = "trailing_garbage_data";

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 12);
    ldb_close(&journal);

    // append garbage after last valid record
    FILE *fp = fopen("test.dat", "ab");
    TEST_ASSERT(fp != NULL);
    fwrite(garbage, sizeof(garbage), 1, fp);
    fclose(fp);

    // identified by ldb_open(), trailing data discarded, journal consistent
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_OK);
    TEST_CHECK(ldb_get_range(&journal).max_seqnum == 12);

    // ldb_check() identify trailing garbage in dat file
    TEST_CHECK(ldb_check(&journal, false) == LDB_ERR_CORRUPT_DAT);
    ldb_close(&journal);

    // repair -> garbage zeroized, journal consistent
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true)  == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    ldb_close(&journal);

    // all original entries intact
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 12);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_check_dat_checksum_mismatch(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    // append seqnum 10..13; each "data-XY" is 8 bytes, padding = 0
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 13);
    ldb_close(&journal);

    // corrupt the checksum of record with seqnum=12
    const size_t checksum_offset_12 = sizeof(ldb_header_dat_t)
                                    + 2 * (sizeof(ldb_record_dat_t) + 8)
                                    + offsetof(ldb_record_dat_t, checksum);

    FILE *fp = fopen("test.dat", "r+b");
    TEST_ASSERT(fp != NULL);
    fseek(fp, (long)checksum_offset_12, SEEK_SET);
    uint32_t bad_checksum = 0xDEADBEEFUL;
    fwrite(&bad_checksum, sizeof(bad_checksum), 1, fp);
    fclose(fp);

    // no repair -> error (bad checksum on seqnum=12, trailing seqnum=13 is non-zero)
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_ERR_CHECKSUM);
    ldb_close(&journal);

    // repair -> zeroizes from seqnum=12 onwards -> consistent
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    ldb_close(&journal);

    // seqnum=10..11 preserved, seqnum=12..13 gone
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 11);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_check_idx_trailing_data(void)
{
    ldb_journal_t journal = {0};
    const char garbage[] = "extra_index_bytes";

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 12);
    ldb_close(&journal);

    // append garbage after last valid idx record
    FILE *fp = fopen("test.idx", "ab");
    TEST_ASSERT(fp != NULL);
    fwrite(garbage, sizeof(garbage), 1, fp);
    fclose(fp);

    // repair -> trailing bytes zeroized, journal consistent
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true)  == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    ldb_close(&journal);

    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 12);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_check_idx_invalid_record(void)
{
    ldb_journal_t journal = {0};
    ldb_record_idx_t record_idx = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 14);
    ldb_close(&journal);

    // corrupt the pos field of idx record seqnum=12
    const size_t pos_idx_12 = sizeof(ldb_header_idx_t) + 2 * sizeof(ldb_record_idx_t);

    FILE *fp = fopen("test.idx", "r+b");
    TEST_ASSERT(fp != NULL);
    fseek(fp, (long)pos_idx_12, SEEK_SET);
    fread(&record_idx, sizeof(ldb_record_idx_t), 1, fp);
    record_idx.pos = 0xDEADBEEFULL;  // wrong pos
    fseek(fp, (long)pos_idx_12, SEEK_SET);
    fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, fp);
    fclose(fp);

    // no repair -> error (open read-only to avoid auto-repair)
    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_READONLY) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_ERR_CORRUPT_IDX);
    ldb_close(&journal);

    // repair -> idx rebuilt from dat
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true)  == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    ldb_close(&journal);

    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 14);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_check_idx_missing_records(void)
{
    ldb_journal_t journal = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&journal, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&journal, 10, 14);
    ldb_close(&journal);

    // truncate idx to remove the last 2 records (simulate unflushed append)
    FILE *fp = fopen("test.idx", "r+b");
    TEST_ASSERT(fp != NULL);
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    ftruncate(fileno(fp), size - 2 * (long)sizeof(ldb_record_idx_t));
    fclose(fp);

    // repair -> idx rebuilt with all 5 records
    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(ldb_check(&journal, true)  == LDB_OK);
    TEST_CHECK(ldb_check(&journal, false) == LDB_OK);
    ldb_close(&journal);

    TEST_ASSERT(ldb_open(&journal, "", "test", 0) == LDB_OK);
    TEST_CHECK(journal.state.min_seqnum == 10);
    TEST_CHECK(journal.state.max_seqnum == 14);
    ldb_close(&journal);

    remove("test.dat");
    remove("test.idx");
}

void test_split_nominal_case(void)
{
    char buf[2048] = {0};
    ldb_impl_t src = {0};
    ldb_impl_t a = {0};
    ldb_impl_t b = {0};
    ldb_entry_t entry = {0};

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&src, 10, 19);
    TEST_CHECK(src.state.min_seqnum == 10);
    TEST_CHECK(src.state.max_seqnum == 19);
    ldb_close(&src);

    // split at seqnum 15: A=[10..14], B=[15..19]
    TEST_CHECK(ldb_split("", "test", 15, "test-a", "test-b") == LDB_OK);

    TEST_ASSERT(ldb_open(&a, "", "test-a", 0) == LDB_OK);
    TEST_CHECK(a.state.min_seqnum == 10);
    TEST_CHECK(a.state.max_seqnum == 14);
    TEST_CHECK(ldb_read(&a, 10, &entry, 1, buf, sizeof(buf), NULL) == LDB_OK);
    TEST_CHECK(check_entry(&entry, 10, "data-10"));
    ldb_close(&a);

    TEST_ASSERT(ldb_open(&b, "", "test-b", 0) == LDB_OK);
    TEST_CHECK(b.state.min_seqnum == 15);
    TEST_CHECK(b.state.max_seqnum == 19);
    TEST_CHECK(ldb_read(&b, 15, &entry, 1, buf, sizeof(buf), NULL) == LDB_OK);
    TEST_CHECK(check_entry(&entry, 15, "data-15"));
    ldb_close(&b);

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");
}

void test_split_at_min_seqnum(void)
{
    ldb_impl_t src = {0};
    ldb_impl_t a = {0};
    ldb_impl_t b = {0};

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&src, 10, 19);
    ldb_close(&src);

    // split at min_seqnum=10: A gets empty (header only), B gets all
    TEST_CHECK(ldb_split("", "test", 10, "test-a", "test-b") == LDB_OK);

    TEST_ASSERT(ldb_open(&a, "", "test-a", 0) == LDB_OK);
    TEST_CHECK(a.state.min_seqnum == 0);  // empty
    TEST_CHECK(a.state.max_seqnum == 0);
    ldb_close(&a);

    TEST_ASSERT(ldb_open(&b, "", "test-b", 0) == LDB_OK);
    TEST_CHECK(b.state.min_seqnum == 10);
    TEST_CHECK(b.state.max_seqnum == 19);
    ldb_close(&b);

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");
}

void test_split_at_max_seqnum(void)
{
    ldb_impl_t src = {0};
    ldb_impl_t a = {0};
    ldb_impl_t b = {0};

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&src, 10, 19);
    ldb_close(&src);

    // split at max_seqnum=19: A=[10..18], B=[19..19]
    TEST_CHECK(ldb_split("", "test", 19, "test-a", "test-b") == LDB_OK);

    TEST_ASSERT(ldb_open(&a, "", "test-a", 0) == LDB_OK);
    TEST_CHECK(a.state.min_seqnum == 10);
    TEST_CHECK(a.state.max_seqnum == 18);
    ldb_close(&a);

    TEST_ASSERT(ldb_open(&b, "", "test-b", 0) == LDB_OK);
    TEST_CHECK(b.state.min_seqnum == 19);
    TEST_CHECK(b.state.max_seqnum == 19);
    ldb_close(&b);

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");
}

void test_split_out_of_range(void)
{
    ldb_impl_t src = {0};

    remove("test.dat"); remove("test.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&src, 10, 19);
    ldb_close(&src);

    // seqnum beyond max: ldb_read_record_idx returns LDB_ERR
    TEST_CHECK(ldb_split("", "test", 20, "test-a", "test-b") != LDB_OK);
    TEST_CHECK(access("test-a.dat", F_OK) != 0);  // not created
    TEST_CHECK(access("test-b.dat", F_OK) != 0);

    remove("test.dat"); remove("test.idx");
}

void test_split_output_exists(void)
{
    ldb_impl_t src = {0};
    ldb_impl_t pre = {0};

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&src, 10, 19);
    ldb_close(&src);

    // pre-create test-a.dat
    TEST_ASSERT(ldb_open(&pre, "", "test-a", LDB_OPEN_CREATE) == LDB_OK);
    ldb_close(&pre);

    TEST_CHECK(ldb_split("", "test", 15, "test-a", "test-b") == LDB_ERR_CREATE_DAT);
    TEST_CHECK(access("test-b.dat", F_OK) != 0);  // B not created (A failed first)

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
}

void test_split_metadata_inherited(void)
{
    char buf[LDB_METADATA_LEN] = {0};
    ldb_impl_t src = {0};
    ldb_impl_t a = {0};
    ldb_impl_t b = {0};

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");

    TEST_ASSERT(ldb_open(&src, "", "test", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(ldb_set_meta(&src, "hello", 5) == LDB_OK);
    append_entries(&src, 1, 10);
    ldb_close(&src);

    TEST_ASSERT(ldb_split("", "test", 6, "test-a", "test-b") == LDB_OK);

    TEST_ASSERT(ldb_open(&a, "", "test-a", 0) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&a, buf, sizeof(buf)) == LDB_OK);
    TEST_CHECK(memcmp(buf, "hello", 5) == 0);
    ldb_close(&a);

    memset(buf, 0, sizeof(buf));
    TEST_ASSERT(ldb_open(&b, "", "test-b", 0) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&b, buf, sizeof(buf)) == LDB_OK);
    TEST_CHECK(memcmp(buf, "hello", 5) == 0);
    ldb_close(&b);

    remove("test.dat");  remove("test.idx");
    remove("test-a.dat"); remove("test-a.idx");
    remove("test-b.dat"); remove("test-b.idx");
}

void test_join_invalid_args(void)
{
    // NULL arguments
    TEST_CHECK(ldb_join(NULL,  "j1", "j2", "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("",   NULL,  "j2", "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("",   "j1", NULL,  "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("",   "j1", "j2",  NULL)  == LDB_ERR_ARG);

    // invalid names
    TEST_CHECK(ldb_join("", "bad name", "j2", "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("", "j1", "bad name", "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("", "j1", "j2", "bad name") == LDB_ERR_ARG);

    // duplicate names
    TEST_CHECK(ldb_join("", "j1", "j1", "out") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("", "j1", "j2", "j1") == LDB_ERR_ARG);
    TEST_CHECK(ldb_join("", "j1", "j2", "j2") == LDB_ERR_ARG);
}

void test_join_nominal(void)
{
    char buf[2048] = {0};
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};
    ldb_impl_t out = {0};
    ldb_entry_t entry = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j1, 10, 19);
    TEST_CHECK(j1.state.min_seqnum == 10);
    TEST_CHECK(j1.state.max_seqnum == 19);
    ldb_close(&j1);

    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j2, 20, 29);
    TEST_CHECK(j2.state.min_seqnum == 20);
    TEST_CHECK(j2.state.max_seqnum == 29);
    ldb_close(&j2);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_OK);

    // source journals removed
    TEST_CHECK(access("test1.dat", F_OK) != 0);
    TEST_CHECK(access("test2.dat", F_OK) != 0);

    TEST_ASSERT(ldb_open(&out, "", "test-out", 0) == LDB_OK);
    TEST_CHECK(out.state.min_seqnum == 10);
    TEST_CHECK(out.state.max_seqnum == 29);
    TEST_CHECK(ldb_read(&out, 10, &entry, 1, buf, sizeof(buf), NULL) == LDB_OK);
    TEST_CHECK(check_entry(&entry, 10, "data-10"));
    TEST_CHECK(ldb_read(&out, 25, &entry, 1, buf, sizeof(buf), NULL) == LDB_OK);
    TEST_CHECK(check_entry(&entry, 25, "data-25"));
    ldb_close(&out);

    // reopen to verify persistence
    TEST_ASSERT(ldb_open(&out, "", "test-out", 0) == LDB_OK);
    TEST_CHECK(out.state.min_seqnum == 10);
    TEST_CHECK(out.state.max_seqnum == 29);
    ldb_close(&out);

    remove("test-out.dat"); remove("test-out.idx");
}

void test_join_metadata(void)
{
    char buf[LDB_METADATA_LEN] = {0};
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};
    ldb_impl_t out = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(ldb_set_meta(&j1, "meta1", 5) == LDB_OK);
    append_entries(&j1, 1, 5);
    ldb_close(&j1);

    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    TEST_CHECK(ldb_set_meta(&j2, "meta2", 5) == LDB_OK);
    append_entries(&j2, 6, 10);
    ldb_close(&j2);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_OK);

    TEST_ASSERT(ldb_open(&out, "", "test-out", 0) == LDB_OK);
    TEST_CHECK(ldb_get_meta(&out, buf, sizeof(buf)) == LDB_OK);
    TEST_CHECK(memcmp(buf, "meta1", 5) == 0);  // inherits name1 metadata
    ldb_close(&out);

    remove("test-out.dat"); remove("test-out.idx");
}

void test_join_non_consecutive(void)
{
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j1, 10, 19);
    ldb_close(&j1);

    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j2, 21, 29);  // gap: 20 is missing
    ldb_close(&j2);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_ERR_SEQNUM);

    // source journals left intact
    TEST_CHECK(access("test1.dat", F_OK) == 0);
    TEST_CHECK(access("test2.dat", F_OK) == 0);
    // output not created
    TEST_CHECK(access("test-out.dat", F_OK) != 0);

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
}

void test_join_output_exists(void)
{
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};
    ldb_impl_t pre = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j1, 10, 19);
    ldb_close(&j1);

    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j2, 20, 29);
    ldb_close(&j2);

    // pre-create output
    TEST_ASSERT(ldb_open(&pre, "", "test-out", LDB_OPEN_CREATE) == LDB_OK);
    ldb_close(&pre);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_ERR_CREATE_DAT);

    // source journals left intact
    TEST_CHECK(access("test1.dat", F_OK) == 0);
    TEST_CHECK(access("test2.dat", F_OK) == 0);

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");
}

void test_join_first_empty(void)
{
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};
    ldb_impl_t out = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    // test1 is empty
    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    ldb_close(&j1);

    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j2, 20, 29);
    ldb_close(&j2);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_OK);

    TEST_ASSERT(ldb_open(&out, "", "test-out", 0) == LDB_OK);
    TEST_CHECK(out.state.min_seqnum == 20);
    TEST_CHECK(out.state.max_seqnum == 29);
    ldb_close(&out);

    remove("test-out.dat"); remove("test-out.idx");
}

void test_join_second_empty(void)
{
    ldb_impl_t j1 = {0};
    ldb_impl_t j2 = {0};
    ldb_impl_t out = {0};

    remove("test1.dat"); remove("test1.idx");
    remove("test2.dat"); remove("test2.idx");
    remove("test-out.dat"); remove("test-out.idx");

    TEST_ASSERT(ldb_open(&j1, "", "test1", LDB_OPEN_CREATE) == LDB_OK);
    append_entries(&j1, 10, 19);
    ldb_close(&j1);

    // test2 is empty
    TEST_ASSERT(ldb_open(&j2, "", "test2", LDB_OPEN_CREATE) == LDB_OK);
    ldb_close(&j2);

    TEST_CHECK(ldb_join("", "test1", "test2", "test-out") == LDB_OK);

    TEST_ASSERT(ldb_open(&out, "", "test-out", 0) == LDB_OK);
    TEST_CHECK(out.state.min_seqnum == 10);
    TEST_CHECK(out.state.max_seqnum == 19);
    ldb_close(&out);

    remove("test-out.dat"); remove("test-out.idx");
}

TEST_LIST = {
    { "sizeof()",                      test_sizeof },
    { "crc32()",                       test_crc32 },
    { "version()",                     test_version },
    { "strerror()",                    test_strerror },
    { "is_valid_path()",               test_is_valid_path },
    { "is_valid_name()",               test_is_valid_name },
    { "create_filename()",             test_create_filename },
    { "close()",                       test_close },
    { "open() with invalid args",      test_open_invalid_args },
    { "open() with invalid path",      test_open_invalid_path },
    { "open() with invalid name",      test_open_invalid_name },
    { "open() create journal",         test_open_create },
    { "open() empty journal",          test_open_empty },
    { "open() invl dat header",        test_open_invl_dat_header },
    { "open() and repair (I)",         test_open_and_repair_1 },
    { "open() and repair (II)",        test_open_and_repair_2 },
    { "open() and repair (III)",       test_open_and_repair_3 },
    { "open() 1-entry ok",             test_open_1_entry_ok },
    { "open() 1-entry empty",          test_open_1_entry_empty },
    { "open() dat corrupted",          test_open_dat_corrupted },
    { "open() idx corrupted",          test_open_idx_corrupted },
    { "open() idx missing last entry", test_open_idx_missing_last_entry },
    { "append() invalid args",         test_append_invalid_args },
    { "append() nothing",              test_append_nothing },
    { "append() auto",                 test_append_auto },
    { "append() nominal case",         test_append_nominal_case },
    { "append() broken sequence",      test_append_broken_sequence },
    { "append() lack of data",         test_append_lack_of_data },
    { "read() invalid args",           test_read_invalid_args },
    { "read() empty journal",          test_read_empty },
    { "read() nominal case",           test_read_nominal_case },
    { "range() all",                   test_range_all },
    { "rollback() invalid args",       test_rollback_invalid_args },
    { "rollback() nominal case",       test_rollback_nominal_case },
    { "alloc() all",                   test_alloc_all },
    { "fsync() all",                   test_fsync_all },
    { "meta() all",                    test_meta_all },
    { "readonly() open",               test_readonly_open },
    { "readonly() write ops",          test_readonly_write_ops },
    { "readonly() no flock",           test_readonly_no_flock },
    { "readonly() missing idx",        test_readonly_missing_idx },
    { "flock()",                       test_flock },
    { "check() invalid args",          test_check_invalid_args },
    { "check() valid journal",         test_check_valid_journal },
    { "check() dat trailing data",     test_check_dat_trailing_data },
    { "check() dat checksum mismatch", test_check_dat_checksum_mismatch },
    { "check() idx trailing data",     test_check_idx_trailing_data },
    { "check() idx invalid record",    test_check_idx_invalid_record },
    { "check() idx missing records",   test_check_idx_missing_records },
    { "split() nominal case",          test_split_nominal_case },
    { "split() at min seqnum",         test_split_at_min_seqnum },
    { "split() at max seqnum",         test_split_at_max_seqnum },
    { "split() out of range",          test_split_out_of_range },
    { "split() output exists",         test_split_output_exists },
    { "split() metadata inherited",    test_split_metadata_inherited },
    { "join() invalid args",           test_join_invalid_args },
    { "join() nominal case",           test_join_nominal },
    { "join() metadata inherited",     test_join_metadata },
    { "join() non consecutive",        test_join_non_consecutive },
    { "join() output exists",          test_join_output_exists },
    { "join() first empty",            test_join_first_empty },
    { "join() second empty",           test_join_second_empty },
    { NULL, NULL }
};
