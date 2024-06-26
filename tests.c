#include "acutest.h"

#define LDB_IMPL
#include "logdb.h"

void append_entries(ldb_db_t *db, uint64_t seqnum1, uint64_t seqnum2)
{
    char metadata[128] = {0};
    char data[128] = {0};

    if (seqnum2 < seqnum1)
        seqnum2 = seqnum1;

    while (seqnum1 <= seqnum2)
    {
        snprintf(metadata, sizeof(metadata), "metadata-%d", (int) seqnum1);
        snprintf(data, sizeof(data), "data-%d", (int) seqnum1);

        // timestamp value equals seqnum to the ten
        // examples: 9->0, 11->10, 19->10, 20->20, 321->320, etc.

        ldb_entry_t entry = {
            .seqnum = seqnum1,
            .timestamp = seqnum1 - (seqnum1 % 10),
            .metadata_len = (uint32_t) strlen(metadata) + 1,
            .data_len = (uint32_t) strlen(data) + 1,
            .metadata = metadata,
            .data = data
        };

        TEST_ASSERT(ldb_append(db, &entry, 1, NULL) == LDB_OK);

        seqnum1++;
    }
}

void test_version(void)
{
    const char *version = ldb_version();
    TEST_ASSERT(version != NULL);

    size_t len = strlen(version);
    TEST_ASSERT(len >= 5);

    TEST_ASSERT(version[0] != '.');
    TEST_ASSERT(version[len-1] != '.');

    size_t num_dots = 0;
    for (size_t i = 0; i < len; i++) {
        if (version[i] == '.')
            num_dots++;
        else if (!isdigit(version[i]))
            TEST_ASSERT(false);
    }

    TEST_CHECK(num_dots == 2);
}

void test_strerror(void)
{
    const char *success = ldb_strerror(LDB_OK);

    TEST_ASSERT(ldb_strerror(0) == ldb_strerror(LDB_OK));
    TEST_ASSERT(strcmp(ldb_strerror(0), success) == 0);

    const char *unknown_error = ldb_strerror(-999);
    TEST_ASSERT(unknown_error != NULL);

    for (int i = 0; i < 21; i++) {
        TEST_ASSERT(ldb_strerror(-i) != NULL);
        TEST_ASSERT(strcmp(ldb_strerror(-i), unknown_error) != 0);
    }
    for (int i = 21; i < 32; i++) {
        TEST_ASSERT(ldb_strerror(-i) != NULL);
        TEST_ASSERT(strcmp(ldb_strerror(-i), unknown_error) == 0);
    }
    for (int i = 1; i < 32; i++) {
        TEST_ASSERT(ldb_strerror(i) != NULL);
        TEST_ASSERT(strcmp(ldb_strerror(i), success) == 0);
    }
}

// Results validated using https://crccalc.com/
void test_crc32(void)
{
    // abnormal cases
    TEST_ASSERT(ldb_crc32(NULL, 0, 42) == 42);
    TEST_ASSERT(ldb_crc32(NULL, 10, 42) == 42);
    TEST_ASSERT(ldb_crc32("", 0, 42) == 42);

    // basic case
    const char str1[] = "hello world";
    TEST_ASSERT(ldb_crc32(str1, strlen(str1), 0) == 0x0D4A1185);

    // composability
    const char str11[] = "hello ";
    const char str12[] = "world";
    size_t checksum = ldb_crc32(str11, strlen(str11), 0);
    checksum = ldb_crc32(str12, strlen(str12), checksum);
    TEST_ASSERT(checksum == 0x0D4A1185);
}

void test_get_millis(void)
{
    uint64_t t0 = 1713331281361; // 17-apr-2024 05:21:21.361 (UTC)
    uint64_t t1 = 2028864081361; // 17-apr-2034 05:21:21.361 (UTC)
    TEST_ASSERT(t0 < ldb_get_millis());
    TEST_ASSERT(ldb_get_millis() < t1);
}

void test_is_valid_path(void)
{
    TEST_ASSERT(ldb_is_valid_path(""));
    TEST_ASSERT(ldb_is_valid_path("."));
    TEST_ASSERT(ldb_is_valid_path("./"));
    TEST_ASSERT(ldb_is_valid_path("/tmp"));
    TEST_ASSERT(ldb_is_valid_path("/tmp/"));
    TEST_ASSERT(ldb_is_valid_path("//tmp"));

    TEST_ASSERT(!ldb_is_valid_path(NULL));
    TEST_ASSERT(!ldb_is_valid_path("/non_existent_dir/"));
    TEST_ASSERT(!ldb_is_valid_path("/etc/passwd"));
}

void test_is_valid_name(void)
{
    TEST_ASSERT(ldb_is_valid_name("test"));
    TEST_ASSERT(ldb_is_valid_name("test_1"));
    TEST_ASSERT(ldb_is_valid_name("_"));
    TEST_ASSERT(ldb_is_valid_name("a"));
    TEST_ASSERT(ldb_is_valid_name("abc"));

    TEST_ASSERT(!ldb_is_valid_name(NULL));
    TEST_ASSERT(!ldb_is_valid_name(""));
    TEST_ASSERT(!ldb_is_valid_name("too_long_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));

    char name[] = "x";
    for (int i = 0; i < 256; i++) {
        name[0] = (char) i;
        TEST_ASSERT(ldb_is_valid_name(name) == (isalnum(i) || name[0] == '_'));
    }
}

void test_create_filename(void)
{
    char *filename = NULL;

    TEST_ASSERT(ldb_create_filename(NULL, "name", ".ext") == NULL);
    TEST_ASSERT(ldb_create_filename("path", NULL, ".ext") == NULL);
    TEST_ASSERT(ldb_create_filename("path", "name", NULL) == NULL);
    TEST_ASSERT(ldb_create_filename("path", "",   ".ext") == NULL);

    filename = ldb_create_filename("path", "name", ".ext");
    TEST_ASSERT(filename != NULL && strcmp(filename, "path/name.ext") == 0);
    free(filename);

    filename = ldb_create_filename("path/", "name", ".ext");
    TEST_ASSERT(filename != NULL && strcmp(filename, "path/name.ext") == 0);
    free(filename);

    filename = ldb_create_filename("", "name", ".ext");
    TEST_ASSERT(filename != NULL && strcmp(filename, "name.ext") == 0);
    free(filename);
}

void test_close(void)
{
    ldb_db_t db = {0};

    TEST_ASSERT(ldb_close(NULL) == LDB_OK);
    TEST_ASSERT(ldb_close(&db) == LDB_OK);

    db.name = (char *) malloc(10);
    db.path = (char *) malloc(10);
    db.dat_path = (char *) malloc(10);
    db.idx_path = (char *) malloc(10);
    db.dat_fp = NULL;
    db.idx_fp = NULL;

    TEST_ASSERT(ldb_close(&db) == LDB_OK);
    TEST_ASSERT(db.name == NULL);
    TEST_ASSERT(db.path == NULL);
    TEST_ASSERT(db.dat_path == NULL);
    TEST_ASSERT(db.idx_path == NULL);
    TEST_ASSERT(db.dat_fp == NULL);
    TEST_ASSERT(db.idx_fp == NULL);
}

void test_open_invalid_args(void) {
    ldb_db_t db = {0};
    TEST_ASSERT(ldb_open(&db , NULL   , "test", false) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_open(&db , "/tmp/",  NULL , false) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_open(NULL, "/tmp/", "test", false) == LDB_ERR_ARG);
}

void test_open_invalid_path(void) {
    ldb_db_t db = {0};
    TEST_ASSERT(ldb_open(&db, "/etc/passwd/", "test", false) == LDB_ERR_PATH);
    TEST_ASSERT(ldb_open(&db, "/non_existent_path/", "test", false) == LDB_ERR_PATH);
}

void test_open_invalid_name(void) {
    ldb_db_t db = {0};
    TEST_ASSERT(ldb_open(&db, "/tmp/", "", false) == LDB_ERR_NAME);
    TEST_ASSERT(ldb_open(&db, "/tmp/", ".", false) == LDB_ERR_NAME);
    TEST_ASSERT(ldb_open(&db, "/tmp/", "xxx-3", false) == LDB_ERR_NAME);
    TEST_ASSERT(ldb_open(&db, "/tmp/", "xxx?", false) == LDB_ERR_NAME);
    TEST_ASSERT(ldb_open(&db, "/tmp/", "too_long_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", false) == LDB_ERR_NAME);
}

void test_open_create_db(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.name != NULL && strcmp(db.name, "test") == 0);
    TEST_ASSERT(db.path != NULL && strcmp(db.path, "") == 0);
    TEST_ASSERT(db.dat_path != NULL && strcmp(db.dat_path, "test.dat") == 0);
    TEST_ASSERT(db.idx_path != NULL && strcmp(db.idx_path, "test.idx") == 0);
    TEST_ASSERT(db.dat_fp != NULL);
    TEST_ASSERT(db.idx_fp != NULL);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.timestamp1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    TEST_ASSERT(db.state.timestamp2 == 0);
    TEST_ASSERT(db.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&db);
}

void test_open_empty_db(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    // create db
    ldb_create_file_dat("test.dat");

    // open empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.name != NULL && strcmp(db.name, "test") == 0);
    TEST_ASSERT(db.path != NULL && strcmp(db.path, "") == 0);
    TEST_ASSERT(db.dat_path != NULL && strcmp(db.dat_path, "test.dat") == 0);
    TEST_ASSERT(db.idx_path != NULL && strcmp(db.idx_path, "test.idx") == 0);
    TEST_ASSERT(db.dat_fp != NULL);
    TEST_ASSERT(db.idx_fp != NULL);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.timestamp1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    TEST_ASSERT(db.state.timestamp2 == 0);
    TEST_ASSERT(db.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&db);
}

void test_open_invl_dat_header(void)
{
    FILE *fp = NULL;
    ldb_db_t db = {0};
    ldb_header_dat_t header = {
        .magic_number = LDB_MAGIC_NUMBER,
        .format = LDB_FORMAT_1,
        .text = {0}
    };

    remove("test.dat");
    remove("test.idx");

    // empty file
    fp = fopen("test.dat", "w");
    fclose(fp);
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_ERR_FMT_DAT);

    // invalid magic numer
    fp = fopen("test.dat", "w");
    header.magic_number = 123;
    fwrite(&header, sizeof(ldb_header_dat_t), 1, fp);
    fclose(fp);
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_ERR_FMT_DAT);

    // invalid file format
    fp = fopen("test.dat", "w");
    header.magic_number = LDB_MAGIC_NUMBER;
    header.format = LDB_FORMAT_1 + 1;
    fwrite(&header, sizeof(ldb_header_dat_t), 1, fp);
    fclose(fp);
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_ERR_FMT_DAT);
}

void test_open_and_repair_1(void)
{
    ldb_db_t db = {0};
    ldb_record_dat_t record = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // writing invalid data (first record too short)
    const char garbage[] = "ioscm,nswddljkh";
    fwrite(garbage, sizeof(garbage), 1, db.dat_fp);
    ldb_close(&db);

    // incomplete record is zeroized
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    ldb_close(&db);

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    // writing invalid first record
    record.seqnum = 1;
    record.timestamp = 0;
    record.data_len = 1000;    // has data length but data not added after record
    record.metadata_len = 54;  // has data length but data not added after record
    fwrite(&record, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    ldb_close(&db);

    // first entry zeroized
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 0);
    ldb_close(&db);
}

void test_open_and_repair_2(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 1 valid entry
    const char data[32000] = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .timestamp = 3,
        .metadata_len = 10,
        .data_len = 21640,
        .metadata = (char *) data,
        .data = (char *) data + 10
    };
    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_OK);

    // inserting a partially zeroized entry
    ldb_record_dat_t record = {
        .seqnum = 0,
        .timestamp = 0,
        .metadata_len = 40,
        .data_len = 400,
    };
    fwrite(&record, sizeof(ldb_record_dat_t), 1, db.dat_fp);

    // inserting garbage
    const char garbage[] = "ioscm,nswddljk";
    fwrite(&garbage, sizeof(garbage), 1, db.dat_fp);
    ldb_close(&db);

    // incomplete record is zeroized
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum2 == 10);

    ldb_close(&db);
}

void test_open_and_repair_3(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 1 valid entry
    const char data[1024] = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .timestamp = 3,
        .metadata_len = 40,
        .data_len = 400,
        .metadata = (char *) data,
        .data = (char *) data + 40
    };
    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_OK);

    // inserting 1 'valid' entry with invalid data length
    ldb_record_dat_t record = {
        .seqnum = entry.seqnum + 1,
        .timestamp = 3,
        .metadata_len = 40,
        .data_len = 400,
        .checksum = 999       // invalid but checked after data length
    };
    fwrite(&record, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    fwrite(data, record.metadata_len + record.data_len - 10, 1, db.dat_fp);
    ldb_close(&db);

    // second record (incomplete) is zeroized
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum2 == 10);

    ldb_close(&db);
}

void test_open_1_entry_ok(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 1 entry
    const char metadata[] = "metadata-1";
    const char data[] = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    ldb_entry_t entry = {
        .seqnum = 10,
        .timestamp = 3,
        .metadata_len = (uint32_t) strlen(metadata),
        .data_len = (uint32_t) strlen(data),
        .metadata = (char *) metadata,
        .data = (char *) data
    };
    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_OK);
    ldb_close(&db);

    // open db with 1-entry (idx will be rebuild)
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.timestamp1 == 3);
    TEST_ASSERT(db.state.seqnum2 == 10);
    TEST_ASSERT(db.state.timestamp2 == 3);
    TEST_ASSERT(db.dat_end == sizeof(ldb_header_dat_t) + sizeof(ldb_record_dat_t) + entry.metadata_len + entry.data_len);
    ldb_close(&db);

    // open db with 1-entry (idx no rebuilded)
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    ldb_close(&db);
}

void test_open_1_entry_empty(void)
{
    const char data[128] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record = {0};

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 1 entry (empty)
    fwrite(&record, sizeof(ldb_record_dat_t), 1, db.dat_fp);

    // inserting additional empty content
    fwrite(data, sizeof(data), 1, db.dat_fp);
    ldb_close(&db);

    // open db with 1-entry (idx will be rebuild)
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.timestamp1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    TEST_ASSERT(db.state.timestamp2 == 0);
    TEST_ASSERT(db.dat_end == sizeof(ldb_header_dat_t));
    ldb_close(&db);
}

void _test_open_rollbacked_ok(bool check)
{
    char data[1024] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record_dat = {0};
    ldb_record_idx_t record_idx = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 4 entries
    for(int i = 10; i < 14; i++)
    {
        record_dat.seqnum = i;
        record_dat.timestamp = 1000 + i;
        record_dat.metadata_len = 6;
        record_dat.data_len = 20 + i;
        checksum = ldb_checksum_record(&record_dat);
        record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);

        record_idx.seqnum = record_dat.seqnum;
        record_idx.timestamp = record_dat.timestamp;
        record_idx.pos = ftell(db.dat_fp);

        fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
        fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

        fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, db.idx_fp);
    }

    // inserting rollbacked info
    memset(data, 0x00, sizeof(data));
    fwrite(data, 60, 1, db.dat_fp);
    fwrite(data, 37, 1, db.idx_fp);
    ldb_close(&db);

    // open database
    TEST_ASSERT(ldb_open(&db, "", "test", check) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.timestamp1 == 1010);
    TEST_ASSERT(db.state.seqnum2 == 13);
    TEST_ASSERT(db.state.timestamp2 == 1013);
    ldb_close(&db);
}

void test_open_rollbacked_ok_check(void) {
    _test_open_rollbacked_ok(true);
}

void test_open_rollbacked_ok_uncheck(void) {
    _test_open_rollbacked_ok(false);
}

void test_open_dat_check_fails(void)
{
    const char data[1024] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record_dat = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting entry-1
    record_dat.seqnum = 10;
    record_dat.timestamp = 10;
    record_dat.metadata_len = 6;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

    // inserting entry-2 (broken sequence)
    record_dat.seqnum = 16;
    record_dat.timestamp = 10;
    record_dat.metadata_len = 6;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

    ldb_close(&db);

    // open database
    TEST_ASSERT(ldb_open(&db, "", "test", true) == LDB_ERR_FMT_DAT);
}

void test_open_dat_corrupted(void)
{
    const char data[1024] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record_dat = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting entry-1
    record_dat.seqnum = 10;
    record_dat.timestamp = 10;
    record_dat.metadata_len = 6;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

    // inserting entry-2 (incorrect checksum)
    record_dat.seqnum = 11;
    record_dat.timestamp = 11;
    record_dat.metadata_len = 6;
    record_dat.data_len = 20;
    checksum = ldb_checksum_record(&record_dat);
    record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum) + 999;
    fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
    fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

    ldb_close(&db);

    // open database
    TEST_ASSERT(ldb_open(&db, "", "test", true) == LDB_ERR_CHECKSUM);
}

void test_open_idx_check_fails_1(void)
{
    const char data[1024] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record_dat = {0};
    ldb_record_idx_t record_idx = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 4 entries
    for(int i = 10; i < 14; i++)
    {
        record_dat.seqnum = i;
        record_dat.timestamp = 1000 + i;
        record_dat.metadata_len = 6;
        record_dat.data_len = 20 + i;
        checksum = ldb_checksum_record(&record_dat);
        record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);

        record_idx.seqnum = record_dat.seqnum + (i == 12 ? 5 : 0); // seqnum mismatch
        record_idx.timestamp = record_dat.timestamp;
        record_idx.pos = ftell(db.dat_fp);

        fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
        fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

        fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, db.idx_fp);
    }
    ldb_close(&db);

    // open database (finish OK due to idx rebuild)
    TEST_ASSERT(ldb_open(&db, "", "test", true) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.timestamp1 == 1010);
    TEST_ASSERT(db.state.seqnum2 == 13);
    TEST_ASSERT(db.state.timestamp2 == 1013);
    ldb_close(&db);
}

void test_open_idx_check_fails_2(void)
{
    char data[1024] = {0};
    ldb_db_t db = {0};
    ldb_record_dat_t record_dat = {0};
    ldb_record_idx_t record_idx = {0};
    uint32_t checksum = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // inserting 4 entries
    for(int i = 10; i < 14; i++)
    {
        record_dat.seqnum = i;
        record_dat.timestamp = 1000 + i;
        record_dat.metadata_len = 6;
        record_dat.data_len = 20 + i;
        checksum = ldb_checksum_record(&record_dat);
        record_dat.checksum = ldb_crc32(data, record_dat.metadata_len + record_dat.data_len, checksum);

        record_idx.seqnum = record_dat.seqnum;
        record_idx.timestamp = record_dat.timestamp;
        record_idx.pos = ftell(db.dat_fp) + (i == 12 ? 5 : 0); // invalid pos

        fwrite(&record_dat, sizeof(ldb_record_dat_t), 1, db.dat_fp);
        fwrite(data, record_dat.metadata_len + record_dat.data_len, 1, db.dat_fp);

        fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, db.idx_fp);
    }
    ldb_close(&db);

    // open database (finish OK due to idx rebuild)
    TEST_ASSERT(ldb_open(&db, "", "test", true) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.timestamp1 == 1010);
    TEST_ASSERT(db.state.seqnum2 == 13);
    TEST_ASSERT(db.state.timestamp2 == 1013);
    ldb_close(&db);
}

void test_alloc_free_entry(void)
{
    char *ptr = NULL;
    ldb_entry_t entry = {0};

    TEST_ASSERT(!ldb_alloc_entry(NULL, 0, 0));
    ldb_free_entry(NULL);

    TEST_ASSERT(ldb_alloc_entry(&entry, 0, 0));
    TEST_ASSERT(entry.data == NULL);
    TEST_ASSERT(entry.metadata == NULL);
    TEST_ASSERT(entry.data_len == 0);
    TEST_ASSERT(entry.metadata_len == 0);
    ldb_free_entry(&entry);

    TEST_ASSERT(ldb_alloc_entry(&entry, 7, 11));
    TEST_ASSERT(entry.metadata_len == 7);
    TEST_ASSERT(entry.data_len == 11);
    TEST_ASSERT((size_t)(entry.metadata) % sizeof(void*) == 0);
    TEST_ASSERT((size_t)(entry.data) % sizeof(void*) == 0);
    ldb_free_entry(&entry);

    TEST_ASSERT(ldb_alloc_entry(&entry, 7, 11));
    ptr = entry.metadata;
    TEST_ASSERT(ldb_alloc_entry(&entry, 2, 5));
    TEST_ASSERT(entry.metadata_len == 2);
    TEST_ASSERT(entry.data_len == 5);
    TEST_ASSERT(entry.metadata == ptr);
    TEST_ASSERT(entry.data == ptr + 8);
    ldb_free_entry(&entry);

    TEST_ASSERT(ldb_alloc_entry(&entry, 0, 11));
    TEST_ASSERT(entry.metadata_len == 0);
    TEST_ASSERT(entry.data_len == 11);
    TEST_ASSERT(entry.metadata != NULL);
    TEST_ASSERT(entry.metadata == entry.data);
    ptr = entry.metadata;

    TEST_ASSERT(ldb_alloc_entry(&entry, 2, 5));
    TEST_ASSERT(entry.metadata_len == 2);
    TEST_ASSERT(entry.data_len == 5);
    TEST_ASSERT(entry.metadata == ptr);
    TEST_ASSERT(entry.data == ptr + sizeof(void*));
    ldb_free_entry(&entry);

    TEST_ASSERT(ldb_alloc_entry(&entry, 11, 0));
    TEST_ASSERT(entry.metadata_len == 11);
    TEST_ASSERT(entry.data_len == 0);
    TEST_ASSERT(entry.metadata != NULL);
    TEST_ASSERT(entry.data == entry.metadata);
    ptr = entry.metadata;

    // used to 'force' an allocation just after the previous one
    // avoiding a realloc was done in the same place
    char *aux = (char *) calloc(1000,1);

    TEST_ASSERT(ldb_alloc_entry(&entry, 2, 5000));
    TEST_ASSERT(entry.metadata_len == 2);
    TEST_ASSERT(entry.data_len == 5000);
    TEST_ASSERT(entry.metadata != ptr); // this test can fail if mem is reallocated in the same place
    TEST_ASSERT((char *) entry.data == (char *) entry.metadata + sizeof(void*));
    ldb_free_entry(&entry);

    free(aux);
}

void test_alloc_free_entries(void)
{
    ldb_entry_t entries[3] = {{0}};

    // abnormal cases are considered (do nothing)
    ldb_free_entries(NULL, 3);
    ldb_free_entries(entries, 0);

    for (int i = 0; i < 3; i++) {
        entries[i].metadata = (char *) malloc(10);
        entries[i].data = (char *) entries[i].metadata + 4;
    }

    ldb_free_entries(entries, 3);

    for (int i = 0; i < 3; i++) {
        TEST_ASSERT(entries[i].metadata == NULL);
        TEST_ASSERT(entries[i].data == NULL);
    }
}

void test_append_invalid_args(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entry = {0};

    TEST_ASSERT(ldb_append(NULL, &entry, 1, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_append(&db, NULL, 1, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_ERR);
}

void test_append_nothing(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entries[10];
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // append 0 entries
    TEST_ASSERT(ldb_append(&db, entries, 0, &num) == LDB_OK);
    TEST_ASSERT(num == 0);

    ldb_close(&db);
}

void test_append_auto(void)
{
    ldb_db_t db = {0};
    const size_t len = 3;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
        entries[i].timestamp = 0;
        snprintf(buf, sizeof(buf), "metadata-%d", (int) i);
        entries[i].metadata = strdup(buf);
        entries[i].metadata_len = strlen(buf) + 1;
        snprintf(buf, sizeof(buf), "data-%d", (int) i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    // append 3 entries
    TEST_ASSERT(ldb_append(&db, entries, len, &num) == LDB_OK);
    TEST_ASSERT(num == len);
    TEST_ASSERT(db.state.seqnum1 == 1);
    TEST_ASSERT(db.state.seqnum2 == len);
    TEST_ASSERT(entries[0].seqnum == 1);
    TEST_ASSERT(entries[1].seqnum == 2);
    TEST_ASSERT(entries[2].seqnum == 3);
    TEST_ASSERT(entries[0].timestamp > 0);
    TEST_ASSERT(entries[1].timestamp > 0);
    TEST_ASSERT(entries[2].timestamp > 0);

    // append 3 entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
        entries[i].timestamp = 0;
    }
    TEST_ASSERT(ldb_append(&db, entries, len, &num) == LDB_OK);
    TEST_ASSERT(num == len);
    TEST_ASSERT(db.state.seqnum1 == 1);
    TEST_ASSERT(db.state.seqnum2 == 2*len);
    TEST_ASSERT(entries[0].seqnum == 4);
    TEST_ASSERT(entries[1].seqnum == 5);
    TEST_ASSERT(entries[2].seqnum == 6);
    TEST_ASSERT(entries[0].timestamp > 0);
    TEST_ASSERT(entries[1].timestamp > 0);
    TEST_ASSERT(entries[2].timestamp > 0);

    ldb_close(&db);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++) {
        free(entries[i].metadata);
        free(entries[i].data);
    }
}

void test_append_nominal_case(void)
{
    ldb_db_t db = {0};
    const size_t len = 10;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 10 + i;
        entries[i].timestamp = 10000 + i;
        snprintf(buf, sizeof(buf), "metadata-%d", (int) i);
        entries[i].metadata = strdup(buf);
        entries[i].metadata_len = strlen(buf) + 1;
        snprintf(buf, sizeof(buf), "data-%d", (int) i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    TEST_ASSERT(ldb_append(&db, entries, len, &num) == LDB_OK);
    TEST_ASSERT(num == len);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.seqnum2 == 10 + len - 1);

    ldb_close(&db);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++) {
        free(entries[i].metadata);
        free(entries[i].data);
    }
}

void test_append_broken_sequence(void)
{
    ldb_db_t db = {0};
    const size_t len = 10;
    ldb_entry_t entries[len];
    char buf[1024] = {0};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    // create entries
    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 10 + i + (i == 5 ? 40 : 0);
        entries[i].timestamp = 10000 + i;
        snprintf(buf, sizeof(buf), "metadata-%d", (int) i);
        entries[i].metadata = strdup(buf);
        entries[i].metadata_len = strlen(buf) + 1;
        snprintf(buf, sizeof(buf), "data-%d", (int) i);
        entries[i].data = strdup(buf);
        entries[i].data_len = strlen(buf) + 1;
    }

    TEST_ASSERT(ldb_append(&db, entries, len, &num) == LDB_ERR_ENTRY_SEQNUM);
    TEST_ASSERT(num == 5);
    TEST_ASSERT(db.state.seqnum1 == 10);
    TEST_ASSERT(db.state.seqnum2 == 10 + num - 1);

    ldb_close(&db);

    // dealloc entries ()
    for (size_t i = 0; i < len; i++) {
        free(entries[i].metadata);
        free(entries[i].data);
    }
}

void test_append_lack_of_data(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entry = {
        .seqnum = 10,
        .timestamp = 1000,
        .metadata_len = 0,
        .data_len = 0,
        .metadata = NULL,
        .data = NULL
    };

    remove("test.dat");
    remove("test.idx");

    // create empty db
    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    entry.metadata_len = 40;

    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_ERR_ENTRY_METADATA);
    entry.metadata_len = 0;
    entry.data_len = 40;

    TEST_ASSERT(ldb_append(&db, &entry, 1, NULL) == LDB_ERR_ENTRY_DATA);

    ldb_close(&db);
}

bool check_entry(ldb_entry_t *entry, uint64_t seqnum, const char *metadata, const char *data)
{
    return (entry && 
            entry->seqnum == seqnum &&
            entry->data_len == (data == NULL ? 0 : strlen(data) + 1) &&
            entry->metadata_len == (metadata == NULL ? 0 : strlen(metadata) + 1) &&
            (entry->data == data || (entry->data != NULL && data != NULL && strcmp(entry->data, data) == 0)) &&
            (entry->metadata == metadata || (entry->metadata != NULL && metadata != NULL && strcmp(entry->metadata, metadata) == 0)));
}

void test_read_invalid_args(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entries[3] = {{0}};

    TEST_ASSERT(ldb_read(NULL, 1, entries, 3, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_read(&db, 1, NULL, 3, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_read(&db, 1, entries, 0, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_read(&db, 1, entries, 3, NULL) == LDB_ERR);
}

void test_read_empty_db(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entries[3] = {{0}};
    size_t num = 10;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    entries[0].seqnum = 5;
    entries[1].seqnum = 15;
    entries[2].seqnum = 25;

    TEST_ASSERT(ldb_read(&db, 0, entries, 3, &num) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(num == 0);

    TEST_ASSERT(ldb_read(&db, 2, entries, 3, &num) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(num == 0);

    ldb_close(&db);
}

void test_read_nominal_case(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entries[10] = {{0}};
    size_t num = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    append_entries(&db, 20, 314);

    TEST_ASSERT(ldb_read(&db, 0, entries, 3, &num) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(num == 0);

    TEST_ASSERT(ldb_read(&db, 10, entries, 3, &num) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(num == 0);

    TEST_ASSERT(ldb_read(&db, 20, entries, 3, &num) == LDB_OK);
    TEST_ASSERT(num == 3);
    TEST_ASSERT(check_entry(&entries[0], 20, "metadata-20", "data-20"));
    TEST_ASSERT(check_entry(&entries[1], 21, "metadata-21", "data-21"));
    TEST_ASSERT(check_entry(&entries[2], 22, "metadata-22", "data-22"));

    TEST_ASSERT(ldb_read(&db, 40, entries, 2, &num) == LDB_OK);
    TEST_ASSERT(num == 2);
    TEST_ASSERT(check_entry(&entries[0], 40, "metadata-40", "data-40"));
    TEST_ASSERT(check_entry(&entries[1], 41, "metadata-41", "data-41"));

    TEST_ASSERT(ldb_read(&db, 313, entries, 3, &num) == LDB_OK);
    TEST_ASSERT(num == 2);
    TEST_ASSERT(check_entry(&entries[0], 313, "metadata-313", "data-313"));
    TEST_ASSERT(check_entry(&entries[1], 314, "metadata-314", "data-314"));

    TEST_ASSERT(ldb_read(&db, 400, entries, 3, &num) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(num == 0);

    ldb_free_entries(entries, 3);
    ldb_close(&db);
}

void test_stats_invalid_args(void)
{
    ldb_db_t db = {0};
    ldb_stats_t stats = {0};

    TEST_ASSERT(ldb_stats(NULL, 1, 1000, &stats) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_stats(&db, 1, 1000, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_stats(&db, 99, 1, &stats) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_stats(&db, 1, 1000, &stats) == LDB_ERR);
}

void test_stats_nominal_case(void)
{
    ldb_db_t db = {0};
    ldb_stats_t stats = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    append_entries(&db, 20, 314);

    TEST_ASSERT(ldb_stats(&db, 10, 15, &stats) == LDB_OK);
    TEST_ASSERT(stats.min_seqnum == 0);
    TEST_ASSERT(stats.max_seqnum == 0);
    TEST_ASSERT(stats.num_entries == 0);
    TEST_ASSERT(stats.index_size == 0);

    TEST_ASSERT(ldb_stats(&db, 900, 1000, &stats) == LDB_OK);
    TEST_ASSERT(stats.min_seqnum == 0);
    TEST_ASSERT(stats.max_seqnum == 0);
    TEST_ASSERT(stats.num_entries == 0);
    TEST_ASSERT(stats.index_size == 0);

    TEST_ASSERT(ldb_stats(&db, 0, 10000000, &stats) == LDB_OK);
    TEST_ASSERT(stats.min_seqnum == 20);
    TEST_ASSERT(stats.max_seqnum == 314);
    TEST_ASSERT(stats.num_entries == 295);
    TEST_ASSERT(stats.index_size == 7080);
//    TEST_ASSERT(stats.data_size == 13410);

    TEST_ASSERT(ldb_stats(&db, 100, 200, &stats) == LDB_OK);
    TEST_ASSERT(stats.min_seqnum == 100);
    TEST_ASSERT(stats.max_seqnum == 200);
    TEST_ASSERT(stats.num_entries == 101);
    TEST_ASSERT(stats.index_size == 2424);
//    TEST_ASSERT(stats.data_size == 4646);

    ldb_close(&db);
}

void test_search_invalid_args(void)
{
    ldb_db_t db = {0};
    uint64_t seqnum = 0;

    TEST_ASSERT(ldb_search(NULL, 1, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_search(&db, 1, LDB_SEARCH_LOWER, NULL) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_search(&db, 1, (ldb_search_e)(9), &seqnum) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_search(&db, 1, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR);
}

void test_search_nominal_case(void)
{
    ldb_db_t db = {0};
    uint64_t seqnum = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    TEST_ASSERT(ldb_search(&db, 10, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR_NOT_FOUND);

    append_entries(&db, 20, 314);

    // LDB_SEARCH_LOWER
    TEST_ASSERT(ldb_search(&db, 0, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 20);
    TEST_ASSERT(ldb_search(&db, 10, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 20);
    TEST_ASSERT(ldb_search(&db, 20, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 20);
    TEST_ASSERT(ldb_search(&db, 25, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 30);
    TEST_ASSERT(ldb_search(&db, 30, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 30);
    TEST_ASSERT(ldb_search(&db, 295, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 300);
    TEST_ASSERT(ldb_search(&db, 300, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 300);
    TEST_ASSERT(ldb_search(&db, 305, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 310);
    TEST_ASSERT(ldb_search(&db, 310, LDB_SEARCH_LOWER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 310);
    TEST_ASSERT(ldb_search(&db, 311, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(ldb_search(&db, 314, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(ldb_search(&db, 999, LDB_SEARCH_LOWER, &seqnum) == LDB_ERR_NOT_FOUND);

    // LDB_SEARCH_UPPER
    TEST_ASSERT(ldb_search(&db, 0, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 20);
    TEST_ASSERT(ldb_search(&db, 10, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 20);
    TEST_ASSERT(ldb_search(&db, 20, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 30);
    TEST_ASSERT(ldb_search(&db, 25, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 30);
    TEST_ASSERT(ldb_search(&db, 30, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 40);
    TEST_ASSERT(ldb_search(&db, 295, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 300);
    TEST_ASSERT(ldb_search(&db, 300, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 310);
    TEST_ASSERT(ldb_search(&db, 305, LDB_SEARCH_UPPER, &seqnum) == LDB_OK);
    TEST_ASSERT(seqnum == 310);
    TEST_ASSERT(ldb_search(&db, 310, LDB_SEARCH_UPPER, &seqnum) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(ldb_search(&db, 311, LDB_SEARCH_UPPER, &seqnum) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(ldb_search(&db, 314, LDB_SEARCH_UPPER, &seqnum) == LDB_ERR_NOT_FOUND);
    TEST_ASSERT(ldb_search(&db, 999, LDB_SEARCH_UPPER, &seqnum) == LDB_ERR_NOT_FOUND);

    ldb_close(&db);
}

void test_rollback_invalid_args(void)
{
    ldb_db_t db = {0};

    TEST_ASSERT(ldb_rollback(NULL, 1) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_rollback(&db, 1) == LDB_ERR);
}

void test_rollback_nominal_case(void)
{
    ldb_db_t db = {0};
    size_t end = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);

    TEST_ASSERT(ldb_rollback(&db, 0) == 0);
    TEST_ASSERT(ldb_rollback(&db, 1) == 0);

    append_entries(&db, 20, 314);
    end = db.dat_end;

    TEST_ASSERT(ldb_rollback(&db, 400) == 0);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);
    TEST_ASSERT(db.dat_end == end);

    TEST_ASSERT(ldb_rollback(&db, 314) == 0);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);
    TEST_ASSERT(db.dat_end == end);

    TEST_ASSERT(ldb_rollback(&db, 313) == 1);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 313);
    TEST_ASSERT(db.dat_end < end);
    end = db.dat_end;

    TEST_ASSERT(ldb_rollback(&db, 100) == 213);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 100);
    TEST_ASSERT(db.dat_end < end);
    end = db.dat_end;

    TEST_ASSERT(ldb_rollback(&db, 20) == 80);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 20);
    TEST_ASSERT(db.dat_end < end);
    end = db.dat_end;

    TEST_ASSERT(ldb_rollback(&db, 0) == 1);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    TEST_ASSERT(db.dat_end < end);

    ldb_close(&db);
}

void test_purge_invalid_args(void)
{
    ldb_db_t db = {0};

    TEST_ASSERT(ldb_purge(NULL, 10) == LDB_ERR_ARG);
    TEST_ASSERT(ldb_purge(&db, 10) == LDB_ERR);
}

void test_purge_empty_db(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(ldb_purge(&db, 10) == 0);
    ldb_close(&db);
}

void test_purge_nothing(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    append_entries(&db, 20, 314);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);

    TEST_ASSERT(ldb_purge(&db, 10) == 0);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);

    ldb_close(&db);
}

void test_purge_nominal_case(void)
{
    ldb_db_t db = {0};
    ldb_entry_t entry = {0};
    size_t dat_end = 0;

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    append_entries(&db, 20, 314);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);
    dat_end = db.dat_end;

    TEST_ASSERT(ldb_purge(&db, 100) == 80);
    TEST_ASSERT(db.state.seqnum1 == 100);
    TEST_ASSERT(db.state.seqnum2 == 314);
    TEST_ASSERT(db.dat_end < dat_end);
    TEST_ASSERT(ldb_read(&db, 101, &entry, 1, NULL) == LDB_OK);
    TEST_ASSERT(entry.seqnum == 101);
    ldb_close(&db);

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 100);
    TEST_ASSERT(db.state.seqnum2 == 314);
    ldb_close(&db);

    ldb_free_entry(&entry);
}

void test_purge_all(void)
{
    ldb_db_t db = {0};

    remove("test.dat");
    remove("test.idx");

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    append_entries(&db, 20, 314);
    TEST_ASSERT(db.state.seqnum1 == 20);
    TEST_ASSERT(db.state.seqnum2 == 314);

    TEST_ASSERT(ldb_purge(&db, 1000) == 295);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    ldb_close(&db);

    TEST_ASSERT(ldb_open(&db, "", "test", false) == LDB_OK);
    TEST_ASSERT(db.state.seqnum1 == 0);
    TEST_ASSERT(db.state.seqnum2 == 0);
    ldb_close(&db);
}

TEST_LIST = {
    { "crc32()",                      test_crc32 },
    { "version()",                    test_version },
    { "strerror()",                   test_strerror },
    { "get_millis()",                 test_get_millis },
    { "is_valid_path()",              test_is_valid_path },
    { "is_valid_name()",              test_is_valid_name },
    { "create_filename()",            test_create_filename },
    { "alloc()/free() entry",         test_alloc_free_entry },
    { "free_entries()",               test_alloc_free_entries },
    { "close()",                      test_close },
    { "open() with invalid args",     test_open_invalid_args },
    { "open() with invalid path",     test_open_invalid_path },
    { "open() with invalid name",     test_open_invalid_name },
    { "open() create db",             test_open_create_db },
    { "open() empty db",              test_open_empty_db },
    { "open() invl dat header",       test_open_invl_dat_header },
    { "open() and repair (I)",        test_open_and_repair_1 },
    { "open() and repair (II)",       test_open_and_repair_2 },
    { "open() and repair (III)",      test_open_and_repair_3 },
    { "open() 1-entry ok",            test_open_1_entry_ok },
    { "open() 1-entry empty",         test_open_1_entry_empty },
    { "open() rollbacked ok",         test_open_rollbacked_ok_uncheck },
    { "open() rollbacked ok (check)", test_open_rollbacked_ok_check },
    { "open() dat check fails",       test_open_dat_check_fails },
    { "open() dat corrupted",         test_open_dat_corrupted },
    { "open() idx check fails (I)",   test_open_idx_check_fails_1 },
    { "open() idx check fails (II)",  test_open_idx_check_fails_2 },
    { "append() invalid args",        test_append_invalid_args },
    { "append() nothing",             test_append_nothing },
    { "append() auto",                test_append_auto },
    { "append() nominal case",        test_append_nominal_case },
    { "append() broken sequence",     test_append_broken_sequence },
    { "append() lack of data",        test_append_lack_of_data },
    { "read() invalid args",          test_read_invalid_args },
    { "read() empty db",              test_read_empty_db },
    { "read() nominal case",          test_read_nominal_case },
    { "stats() invalid args",         test_stats_invalid_args },
    { "stats() nominal case",         test_stats_nominal_case },
    { "search() invalid args",        test_search_invalid_args },
    { "search() nominal case",        test_search_nominal_case },
    { "rollback() invalid args",      test_rollback_invalid_args },
    { "rollback() nominal case",      test_rollback_nominal_case },
    { "purge() invalid args",         test_purge_invalid_args },
    { "purge() empty db",             test_purge_empty_db },
    { "purge() nothing",              test_purge_nothing },
    { "purge() nominal case",         test_purge_nominal_case },
    { "purge() all",                  test_purge_all },
    { NULL, NULL }
};
