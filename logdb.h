/*
MIT License

logdb -- A simple log-structured database.
<https://github.com/torrentg/logdb>

Copyright (c) 2024 Gerard Torrent <gerard@generacio.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef __LOGDB_H
#define __LOGDB_H

#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <stdarg.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/time.h>

#define LDB_VERSION_MAJOR          0
#define LDB_VERSION_MINOR          1
#define LDB_VERSION_PATCH          0

#define LDB_OK                     0
#define LDB_ERR                   -1
#define LDB_ERR_ARG               -2
#define LDB_ERR_MEM               -3
#define LDB_ERR_NAME              -4
#define LDB_ERR_PATH              -5
#define LDB_ERR_FILE              -6
#define LDB_ERR_READ_DAT          -7
#define LDB_ERR_WRITE_DAT         -8
#define LDB_ERR_READ_IDX          -9
#define LDB_ERR_WRITE_IDX        -10
#define LDB_ERR_FMT_DAT          -11
#define LDB_ERR_FMT_IDX          -12
#define LDB_ERR_ENTRY_SEQNUM     -13
#define LDB_ERR_ENTRY_TIMESTAMP  -14
#define LDB_ERR_ENTRY_METADATA   -15
#define LDB_ERR_ENTRY_DATA       -16
#define LDB_ERR_NOT_FOUND        -17

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    LDB_SEARCH_LOWER,        // Returns the seqnum of the first entry having timestamp not less than the given value.
    LDB_SEARCH_UPPER         // Returns the seqnum of the first entry having timestamp greater than the given value.
} ldb_search_e;

typedef enum {
    LDB_STATUS_CLOSED = 0,
    LDB_STATUS_OPEN = 1
} ldb_status_e;

typedef struct {
    char *name;
    char *path;
    char *error;
    char *dat_path;
    FILE *dat_fp;
    char *idx_path;
    FILE *idx_fp;
    uint64_t milestone;
    uint64_t first_seqnum;
    uint64_t first_timestamp;
    uint64_t last_seqnum;
    uint64_t last_timestamp;
    size_t dat_end;
    uint32_t format;
    char state;
} ldb_db_t;

typedef struct {
    uint64_t seqnum;
    uint64_t timestamp;
    uint32_t metadata_len;
    uint32_t data_len;
    char *metadata;
    char *data;
} ldb_entry_t;

typedef struct {
    uint64_t min_seqnum;
    uint64_t max_seqnum;
    uint64_t min_timestamp;
    uint64_t max_timestamp;
    uint64_t num_entries;
    uint64_t data_size;
    uint64_t index_size;
} ldb_stats_t;

/**
 * Returns ldb library version.
 * @return Library version (semantic version, ex. 1.0.4).
 */
const char * ldb_version(void);

/**
 * Deallocates the memory pointed by the entry.
 * 
 * Use this function to deallocate entries returned by ldb_read().
 * Update entry pointers to NULL and lengths to 0.
 * 
 * @param[in,out] entry Entry to dealloc data (if NULL does nothing).
 */
void ldb_free_entry(ldb_entry_t *entry);

/**
 * Deallocates the memory of an array of entries.
 * 
 * This is an utility function that calls ldb_free_entry() for
 * each array item.
 * 
 * @param[in] entries Array of entries (if NULL does nothing).
 * @param[in] len Number of entries.
 */
void ldb_free_entries(ldb_entry_t *entries, size_t len);

/**
 * Open a database.
 * 
 * Creates database files (dat+idx) if they not exists.
 * Update index file if incomplete (not flushed + crash).
 * Rebuild index file when corrupted or not found.
 * 
 * @param[in] path Directory where database files are located.
 * @param[in] name Database name (chars allowed: [a-ZA-Z_], max length = 32).
 * @param[in,out] obj Uninitialized database object.
 * @param[in] check Check database files (true|false).
 * @return Error code (0 = OK). On error db is closed properly (ldb_close not required).
 *         You can check errno value to get additional error details.
 */
int ldb_open(const char *path, const char *name, ldb_db_t *obj, bool check);

/**
 * Close a database.
 * 
 * Close open files and release allocated memory.
 * 
 * @param[in,out] obj Database to close.
 * @return Return code (0 = OK).
 */
int ldb_close(ldb_db_t *obj);

/**
 * Append entries to the database.
 * 
 * This function is not 'atomic'. Entries are appended sequentially. 
 * On error (ex. disk full) writed entries are flushed and remaining entries
 * are reported as not writed (see num return argument).
 * 
 * Seqnum values:
 *   - equals to 0 -> system assigns the sequential value.
 *   - distinct than 0 -> system check that it is the next value.
 * 
 * Timestamp values:
 *   - equals to 0: system assigns the current UTC epoch time (in millis).
 *   - distinct than 0 -> system check that is bigger or equal to previous timestamp.
 * 
 * File operations:
 *   - Data file is updated and flushed.
 *   - Index file is updated but not flushed.
 * 
 * Memory pointed by entries is not modified and can be deallocated after function call.
 * 
 * @param[in] obj Database to modify.
 * @param[in,out] entries Entries to append to the database. Memory pointed 
 *                  by each entry is not modified. Seqnum and timestamp
 *                  are updated if they have value 0.
 *                  User must reset pointers before reuse.
 * @param[in] len Number of entries to append.
 * @param[out] num Number of entries appended (can be NULL).
 * @return Error code (0 = OK).
 */
int ldb_append(ldb_db_t *obj, ldb_entry_t *entries, size_t len, size_t *num);

/**
 * Read num entries starting from seqnum (included).
 * 
 * @param[in] obj Database to use.
 * @param[in] seqnum Initial sequence number. It is set to seqnum if less than first seqnum.
 * @param[out] entries Array of entries (min length = len).
 *                  These entries are uninitialized (with NULL pointers) or entries 
 *                  previously initialized by ldb_read() function. In this case, the 
 *                  allocated memory is reused and will be reallocated if not enough.
 *                  Use ldb_free_entry() to dealloc returned entries.
 * @param[in] len Number of entries to read.
 * @param[out] num Number of entries read (can be NULL). If num less than 'len' means 
 *                  that last record was reached. Unused entries are signaled with 
 *                  seqnum = 0.
 * @return Error code (0 = OK). Can be returned 0 records when the db is empty or the given 
 *                  seqnum is greater than last db seqnum).
 */
int ldb_read(ldb_db_t *obj, uint64_t seqnum, ldb_entry_t *entries, size_t len, size_t *num);

/**
 * Return statistics between seqnum1 and seqnum2 (both included).
 * 
 * @param[in] obj Database to use.
 * @param[in] seqnum1 First sequence number.
 * @param[in] seqnum2 Second sequence number (greater or equal than seqnum1).
 * @param[out] stats Uninitialized statistics.
 * @return Error code (0 = OK).
 */
int ldb_stats(ldb_db_t *obj, uint64_t seqnum1, uint64_t seqnum2, ldb_stats_t *stats);

/**
 * Search the seqnum corresponding to the given timestamp.
 * 
 * Use the binary search algorithm over the index file.
 * 
 * @param[in] obj Database to use.
 * @param[in] ts Timestamp to search.
 * @param[in] mode Search mode.
 * @param[out] seqnum Resulting seqnum (0 = NOT_FOUND).
 * @return Error code (0 = OK).
 */
int ldb_search_by_ts(ldb_db_t *obj, uint64_t ts, ldb_search_e mode, uint64_t *seqnum);

/**
 * Remove all entries greater than seqnum.
 * 
 * File operations:
 *   - Index file is updated and flushed.
 *   - Removed values are replaced by 0's from higher to lower.
 *   - Data file is updated and flushed.
 * 
 * @param[in] obj Database to update.
 * @param[in] seqnum Sequence number from which records are removed.
 * @param[out] num Number of removed entries (can be NULL).
 * @return Error code (0 = OK).
 */
int ldb_rollback(ldb_db_t *obj, uint64_t seqnum, size_t *num);

/**
 * Remove all entries less than seqnum.
 * 
 * To prevent data loss in case of outage we do:
 *   - Index file is removed.
 *   - A temporary data file is created.
 *   - Data is copied from data file to new data file.
 *   - Data file is replaced by the new one.
 *   - Index file is rebuilt.
 * 
 * @param[in] obj Database to update.
 * @param[in] seqnum Sequence number up to which records are removed.
 * @param[out] num Number of removed entries (can be NULL).
 * @return Error code (0 = OK).
 */
int ldb_purge(ldb_db_t *obj, uint64_t seqnum, size_t *num);

/**
 * Update the milestone value.
 * 
 * Database file is flushed before function return.
 * 
 * @param[in] obj Database to use.
 * @param[in] seqnum Milestone value.
 * @return Error code (0 = OK).
 */
int ldb_update_milestone(ldb_db_t *obj, uint64_t seqnum);

#ifdef __cplusplus
}
#endif

#endif /* __LOGDB_H */

/* ------------------------------------------------------------------------- */

#ifdef LDB_IMPL

#define LDB_EXT_DAT             ".dat"
#define LDB_EXT_IDX             ".idx"
#define LDB_EXT_TMP             ".tmp"
#define LDB_PATH_SEPARATOR      "/"
#define LDB_NAME_MAX_LENGTH     32 
#define LDB_TEXT_LEN            128  /* value multiple of 8 to preserve alignment */
#define LDB_TEXT_DAT            "\nThis is a ldb database dat file.\nDon't edit it.\n"
#define LDB_TEXT_IDX            "\nThis is a ldb database idx file.\nDon't edit it.\n"
#define LDB_MAGIC_NUMBER        0x62646C00
#define LDB_FORMAT_1            1

#define LDB_MAX(x, y) (((x) > (y)) ? (x) : (y))
#define LDB_MIN(x, y) (((x) < (y)) ? (x) : (y))
#define LDB_FREE(ptr) do { free(ptr); ptr = NULL; } while(0)

typedef struct {
    uint32_t magic_number;
    char text[LDB_TEXT_LEN];
    uint32_t format;
    uint64_t milestone;
} ldb_header_dat_t;

typedef struct {
    uint32_t magic_number;
    char text[LDB_TEXT_LEN];
    uint32_t format;
} ldb_header_idx_t;

typedef struct {
    uint64_t seqnum;
    uint64_t timestamp;
    uint32_t metadata_len;
    uint32_t data_len;
} ldb_record_dat_t;

typedef struct {
    uint64_t seqnum;
    uint64_t timestamp;
    uint64_t pos;
} ldb_record_idx_t;

static void ldb_set_error(ldb_db_t *obj, const char *fmt, ...)
{
    assert(fmt);

    if (!obj || !fmt)
        return;

    va_list args;
    char msg[1024] = {0};

    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    LDB_FREE(obj->error);
    obj->error = strdup(msg);
}

static uint64_t ldb_get_millis(void)
{
    struct timeval tm = {0};
    gettimeofday(&tm, NULL);
    return (uint64_t)(tm.tv_sec * 1000 + tm.tv_usec / 1000);
}

static uint64_t ldb_clamp(uint64_t val, uint64_t lo, uint64_t hi)
{
    return (val < lo ? lo : (hi < val ? hi : val));
}

static int ldb_close_file_dat(ldb_db_t *obj)
{
    if (!obj || !obj->dat_fp)
        return LDB_OK;

    int ret = LDB_OK;

    if (fclose(obj->dat_fp) != 0) {
        ldb_set_error(obj, "Error closing dat file: %s", strerror(errno));
        ret = LDB_ERR_WRITE_DAT;
    }

    obj->dat_fp = NULL;

    return ret;
}

static int ldb_close_file_idx(ldb_db_t *obj)
{
    if (!obj || !obj->idx_fp)
        return LDB_OK;

    int ret = LDB_OK;

    if (fclose(obj->idx_fp) != 0) {
        ldb_set_error(obj, "Error closing idx file: %s", strerror(errno));
        ret = LDB_ERR_WRITE_IDX;
    }

    obj->idx_fp = NULL;

    return ret;
}

static int ldb_free_db(ldb_db_t *obj)
{
    if (obj == NULL)
        return LDB_OK;

    int rc1 = ldb_close_file_dat(obj);
    int rc2 = ldb_close_file_idx(obj);

    LDB_FREE(obj->name);
    LDB_FREE(obj->path);
    LDB_FREE(obj->error);
    LDB_FREE(obj->dat_path);
    LDB_FREE(obj->idx_path);

    obj->milestone = 0;
    obj->first_seqnum = 0;
    obj->first_timestamp = 0;
    obj->last_seqnum = 0;
    obj->last_timestamp = 0;
    obj->dat_end = 0;

    return (rc1 != LDB_OK ? rc1 : (rc2 != LDB_OK ? rc2 : LDB_OK));
}

void ldb_free_entry(ldb_entry_t *entry)
{
    if (entry == NULL)
        return;
    
    // ldb_alloc_entry() does only 1 mem allocation
    if (entry->metadata != NULL) {
        free(entry->metadata);
    }

    entry->metadata = NULL;
    entry->data = NULL;
    entry->metadata_len = 0;
    entry->data_len = 0;
}

void ldb_free_entries(ldb_entry_t *entries, size_t len)
{
    if (entries == NULL || len == 0)
        return;

    for (size_t i = 0; i < len; i++)
        ldb_free_entry(entries + i);
}

// returns size adjusted to a multiple of sizeof(void*)
static size_t ldb_allocated_size(size_t size)
{
    size_t rem = size % sizeof(void *);
    return size + (rem == 0 ? 0 : sizeof(void *) - rem);
}

// alloc entry memory aligned to generic type
// returns false on error
static bool ldb_alloc_entry(ldb_entry_t *entry, uint32_t metadata_len, uint32_t data_len)
{
    if (entry == NULL)
        return false;

    if ((entry->metadata_len > 0 && entry->metadata == NULL) ||
        (entry->data_len > 0 && entry->data == NULL) ||
        (entry->metadata == NULL && entry->data != NULL))
     {
        assert(false);
        ldb_free_entry(entry);
    }

    size_t len1 = ldb_allocated_size(metadata_len);
    size_t len2 = ldb_allocated_size(data_len);
    assert((len1 + len2) % sizeof(void *) == 0);

    char *ptr = NULL;

    if (len1 + len2 <= ldb_allocated_size(entry->metadata_len) + ldb_allocated_size(entry->data_len)) {
        ptr = entry->metadata ? entry->metadata : (entry->data ? entry->data : NULL);
    }
    else {
        ldb_free_entry(entry);
        ptr = (char *) calloc((len1 + len2)/sizeof(void *), sizeof(void *));
        if (ptr == NULL)
            return false;
    }

    entry->metadata = ptr;
    entry->metadata_len = metadata_len;
    entry->data = ptr + (data_len == 0 ? 0 : len1);
    entry->data_len = data_len;

    return true;
}

static bool ldb_is_valid_path(const char *path)
{
    struct stat statbuf = {0};

    if (path == NULL)
        return false;

    if (*path == 0)  // case cwd (current working directory)
        return true;

    if (stat(path, &statbuf) != 0)
        return false;

    if (!S_ISDIR(statbuf.st_mode))
        return false;

    if (access(path, R_OK) != 0)
        return false;

    return true;
}

static bool ldb_is_valid_name(const char *name)
{
    if (!name || *name == 0)
        return false;

    const char *ptr = name;

    while (*ptr != 0 && (isalnum(*ptr) || *ptr == '_'))
        ptr++;

    return (*ptr == 0 && ptr - name < LDB_NAME_MAX_LENGTH);
}

static char * ldb_create_filename(const char *path, const char *name, const char *ext)
{
    if (path == NULL || name == NULL || ext == NULL || strlen(name) == 0)
        return NULL;

    size_t len = strlen(path);
    bool path_sep_required = (len > 0 && path[len -1] != LDB_PATH_SEPARATOR[0]);

    len = strlen(path) + 1 + strlen(name) + strlen(ext) + 1;
    char *filepath = (char *) calloc(len, sizeof(char));

    if (filepath == NULL)
        return NULL;

    snprintf(filepath, len, "%s%s%s%s", 
             path,
             (path_sep_required ? LDB_PATH_SEPARATOR : ""),
             name,
             ext);

    return filepath;
}

static bool ldb_create_file_dat(const char *path)
{
    assert(path);

    if (access(path, F_OK) == 0)
        return false;

    bool ret = true;

    FILE *fp = fopen(path, "w");
    if (fp == NULL)
        return false;

    ldb_header_dat_t header = {
        .magic_number = LDB_MAGIC_NUMBER,
        .text = {0},
        .format = LDB_FORMAT_1,
        .milestone = 0
    };

    strncpy(header.text, LDB_TEXT_DAT, sizeof(header.text));

    if (fwrite(&header, sizeof(ldb_header_dat_t), 1, fp) != 1)
        ret = false;

    if (fclose(fp) != 0)
        ret = false;

    return ret;
}

static bool ldb_create_file_idx(const char *path)
{
    assert(path);

    if (access(path, F_OK) == 0)
        return false;

    bool ret = true;

    FILE *fp = fopen(path, "w");
    if (fp == NULL)
        return false;

    ldb_header_idx_t header = {
        .magic_number = LDB_MAGIC_NUMBER,
        .text = {0},
        .format = LDB_FORMAT_1
    };

    strncpy(header.text, LDB_TEXT_IDX, sizeof(header.text));

    if (fwrite(&header, sizeof(ldb_header_idx_t), 1, fp) != 1)
        ret = false;

    if (fclose(fp) != 0)
        ret = false;

    return ret;
}

// Check that file content are 0's from current position to end
// Preserve current file position
static bool ldb_is_rollbacked(FILE *fp)
{
    assert(fp);
    assert(!feof(fp));
    assert(!ferror(fp));

    char c = 0;
    long pos = ftell(fp);

    while (!feof(fp) && !ferror(fp))
    {
        fread(&c, sizeof(char), 1, fp);
        if (c != 0)
            return false;
    }

    clearerr(fp);
    fseek(fp, pos, SEEK_SET);

    return true;
}

// Returns file size preserving current file position
// Returns 0 on error
static size_t ldb_get_file_size(FILE *fp)
{
    assert(fp);
    assert(!ferror(fp));

    long pos = ftell(fp);
    if (pos < 0)
        return 0;

    if (fseek(fp, 0, SEEK_END) != 0)
        return 0;

    long len = ftell(fp);

    if (fseek(fp, pos, SEEK_SET) != 0)
        return 0;

    return (size_t)(len < 0 ? 0 : len);
}

static int ldb_append_entry_dat(ldb_db_t *obj, ldb_entry_t *entry)
{
    assert(obj);
    assert(entry);
    assert(obj->dat_fp);
    assert(!feof(obj->dat_fp));
    assert(!ferror(obj->dat_fp));

    if (entry->metadata_len != 0 && entry->metadata == NULL)
	return LDB_ERR_ENTRY_METADATA;

    if (entry->data_len != 0 && entry->data == NULL)
        return LDB_ERR_ENTRY_DATA;

    if (obj->last_seqnum != 0 && entry->seqnum != obj->last_seqnum + 1)
        return LDB_ERR_ENTRY_SEQNUM;
                                                                                                                                 
    if (entry->timestamp < obj->last_timestamp)
        return LDB_ERR_ENTRY_TIMESTAMP;

    ldb_record_dat_t record = {
        .seqnum = entry->seqnum,
        .timestamp = entry->timestamp,
        .metadata_len = entry->metadata_len,
        .data_len = entry->data_len
    };

    if (fseek(obj->dat_fp, obj->dat_end, SEEK_SET) != 0)
        return LDB_ERR_WRITE_DAT;

    if (fwrite(&record, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1)
        return LDB_ERR_WRITE_DAT;

    if (record.metadata_len)
        if (fwrite(entry->metadata, sizeof(char), record.metadata_len, obj->dat_fp) != record.metadata_len)
            return LDB_ERR_WRITE_DAT;

    if (record.data_len)
        if (fwrite(entry->data, sizeof(char), record.data_len, obj->dat_fp) != record.data_len)
            return LDB_ERR_WRITE_DAT;

    if (obj->first_seqnum == 0) {
        obj->first_seqnum = entry->seqnum;
        obj->first_timestamp = entry->timestamp;
    }

    obj->last_seqnum = entry->seqnum;
    obj->last_timestamp = entry->timestamp;
    obj->dat_end = (size_t) ftell(obj->dat_fp);

    return LDB_OK;
}

static int ldb_read_record_dat(ldb_db_t *obj, size_t pos, ldb_record_dat_t *record)
{
    assert(obj);
    assert(record);
    assert(obj->dat_fp);
    assert(!feof(obj->dat_fp));
    assert(!ferror(obj->dat_fp));

    if (fseek(obj->dat_fp, pos, SEEK_SET) != 0)
        return LDB_ERR_READ_DAT;

    if (fread(record, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1)
        return LDB_ERR_READ_DAT;

    if (record->seqnum < obj->first_seqnum || obj->last_seqnum < record->seqnum)
        return LDB_ERR;

    return LDB_OK;
}

static int ldb_read_entry_dat(ldb_db_t *obj, size_t pos, ldb_entry_t *entry)
{
    assert(obj);
    assert(entry);
    assert(obj->dat_fp);
    assert(!feof(obj->dat_fp));
    assert(!ferror(obj->dat_fp));

    int rc = 0;
    ldb_record_dat_t record = {0};

    if ((rc = ldb_read_record_dat(obj, pos, &record)) != LDB_OK)
        return rc;

    ldb_alloc_entry(entry, record.metadata_len, record.data_len);

    if (record.metadata_len) {
        assert(entry->metadata != NULL);
        if (fread(entry->metadata, record.metadata_len, 1, obj->dat_fp) != 1)
            return LDB_ERR_READ_DAT;
    }

    if (record.data_len) {
        assert(entry->data != NULL);
        if (fread(entry->data, record.data_len, 1, obj->dat_fp) != 1)
            return LDB_ERR_READ_DAT;
    }

    entry->seqnum = record.seqnum;
    entry->timestamp = record.timestamp;

    return LDB_OK;
}

static size_t ldb_get_pos_idx(ldb_db_t *obj, uint64_t seqnum)
{
    assert(obj);
    assert(obj->first_seqnum <= seqnum);
    assert(seqnum <= obj->last_seqnum);

    size_t diff = (obj->first_seqnum == 0 ? 0 : seqnum - obj->first_seqnum);
    return sizeof(ldb_header_idx_t) + diff * sizeof(ldb_record_idx_t);
}

static int ldb_append_record_idx(ldb_db_t *obj, ldb_record_idx_t *record)
{
    assert(obj);
    assert(record);
    assert(obj->idx_fp);
    assert(!feof(obj->idx_fp));
    assert(!ferror(obj->idx_fp));
    assert(obj->first_seqnum <= obj->last_seqnum);

    if (record->seqnum != obj->last_seqnum) {
        assert(false);
        return LDB_ERR;
    }

    size_t pos = ldb_get_pos_idx(obj, record->seqnum);

    if (fseek(obj->idx_fp, (long) pos, SEEK_SET) != 0)
        return LDB_ERR_READ_IDX;

    if (fwrite(record, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1)
        return LDB_ERR_WRITE_IDX;

    return LDB_OK;
}

static int ldb_read_record_idx(ldb_db_t *obj, uint64_t seqnum, ldb_record_idx_t *record)
{
    assert(obj);
    assert(record);
    assert(seqnum > 0);

    if (seqnum == obj->first_seqnum) {
        record->seqnum = obj->first_seqnum;
        record->timestamp = obj->first_timestamp;
        record->pos = sizeof(ldb_header_dat_t);
        return LDB_OK;
    }

    if (obj->first_seqnum == 0 || seqnum < obj->first_seqnum || obj->last_seqnum < seqnum)
        return LDB_ERR;

    size_t pos = ldb_get_pos_idx(obj, seqnum);

    if (fseek(obj->idx_fp, (long) pos, SEEK_SET) != 0)
        return LDB_ERR_READ_IDX;

    if (fread(record, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1)
        return LDB_ERR_READ_IDX;

    if (record->seqnum != seqnum)
        return LDB_ERR;

    return LDB_OK;
}


static bool ldb_is_record_dat_empty(const ldb_record_dat_t *record)
{
    return (record && 
            record->seqnum == 0 && record->timestamp == 0 &&
            record->data_len == 0 && record->metadata_len == 0);
}

static bool ldb_is_record_idx_empty(const ldb_record_idx_t *record)
{
    return (record && record->seqnum == 0 && record->timestamp == 0 && record->pos == 0);
}

/**
 * pre-conditions:
 *   - obj->dat_fp == NULL
 *   - obj->idx_fp == NULL
 *
 * post-conditions (OK)
 *   - obj->dat_fp = set
 *   - obj->format = set
 *   - obj->first_seqnum = set (0 if no data)
 *   - obj->first_timestamp = set
 * 
 * post-conditions (KO)
 *   - obj->dat_fp = NULL
 */
static int ldb_open_file_dat(ldb_db_t *obj, bool check)
{
    assert(obj);
    assert(obj->dat_fp == NULL);
    assert(obj->idx_fp == NULL);

    int ret = LDB_OK;
    ldb_header_dat_t header = {0};
    ldb_record_dat_t record = {0};
    size_t pos = 0;
    size_t len = 0;

    obj->first_seqnum = 0;
    obj->first_timestamp = 0;
    obj->last_seqnum = 0;
    obj->last_timestamp = 0;
    obj->dat_end = sizeof(ldb_header_dat_t);

    obj->dat_fp = fopen(obj->dat_path, "r+");

    if (obj->dat_fp == NULL) {
        ldb_set_error(obj, "Cannot open file '%s': %s", obj->dat_path, strerror(errno));
        return LDB_ERR_FILE;
    }

    len = ldb_get_file_size(obj->dat_fp);

    if (fread(&header, sizeof(ldb_header_dat_t), 1, obj->dat_fp) != 1)
        goto LDB_OPEN_FILE_DAT_ERR_FMT;

    pos += sizeof(ldb_header_dat_t);

    if (header.magic_number != LDB_MAGIC_NUMBER) 
        goto LDB_OPEN_FILE_DAT_ERR_FMT;

    if (header.format != LDB_FORMAT_1) {
        ldb_set_error(obj, "Unrecognized database format (dat)");
        ret = LDB_ERR_FMT_DAT;
        goto LDB_OPEN_FILE_DAT_ERR;
    }

    obj->format = header.format;
    obj->milestone = header.milestone;

    if (pos == len)
    {
        // there is no first entry
    }
    else if (pos + sizeof(ldb_record_dat_t) > len)
    {
        if (!ldb_is_rollbacked(obj->dat_fp))
            goto LDB_OPEN_FILE_DAT_ERR_FMT;
    }
    else
    {
        // read first entry
        if (fread(&record, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1) 
            goto LDB_OPEN_FILE_DAT_ERR_READ;

        pos += sizeof(ldb_record_dat_t);

        // case first record is empty
        if (record.seqnum == 0 && !ldb_is_record_dat_empty(&record))
            goto LDB_OPEN_FILE_DAT_ERR_FMT;

        pos += record.metadata_len + record.data_len;

        if (pos > len)
            goto LDB_OPEN_FILE_DAT_ERR_FMT;

        if (fseek(obj->dat_fp, (long) pos, SEEK_SET) != 0)
            goto LDB_OPEN_FILE_DAT_ERR_READ;

        obj->first_seqnum = record.seqnum;
        obj->first_timestamp = record.timestamp;
    }

    if (!check) {
        return LDB_OK;
    }

    obj->last_seqnum = record.seqnum;
    obj->last_timestamp = record.timestamp;

    while (pos + sizeof(ldb_record_dat_t) <= len)
    {
        if (fread(&record, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1) 
            goto LDB_OPEN_FILE_DAT_ERR_READ;

        pos += sizeof(ldb_record_dat_t);

        // case record removed (rollback)
        if (record.seqnum == 0 || obj->first_seqnum == 0)
        {
            if (!ldb_is_record_dat_empty(&record))
                goto LDB_OPEN_FILE_DAT_ERR_FMT;

            break;
        }

        if (record.seqnum != obj->last_seqnum + 1) 
            goto LDB_OPEN_FILE_DAT_ERR_FMT;

        if (record.timestamp < obj->last_timestamp)
            goto LDB_OPEN_FILE_DAT_ERR_FMT;

        pos += record.metadata_len + record.data_len;

        if (pos > len)
            goto LDB_OPEN_FILE_DAT_ERR_FMT;

        if (fseek(obj->dat_fp, (long) pos, SEEK_SET) != 0)
            goto LDB_OPEN_FILE_DAT_ERR_READ;

        obj->last_seqnum = record.seqnum;
        obj->last_timestamp = record.timestamp;
    }

    if (!ldb_is_rollbacked(obj->dat_fp))
        goto LDB_OPEN_FILE_DAT_ERR_FMT;

    return LDB_OK;

LDB_OPEN_FILE_DAT_ERR:
    ldb_close_file_dat(obj);
    return ret;

LDB_OPEN_FILE_DAT_ERR_READ:
    ldb_close_file_idx(obj);
    ldb_set_error(obj, "Error reading file %s%s", obj->name, LDB_EXT_DAT);
    return LDB_ERR_READ_IDX;

LDB_OPEN_FILE_DAT_ERR_FMT:
    ldb_close_file_dat(obj);
    ldb_set_error(obj, "Corrupted file: %s%s", obj->name, LDB_EXT_DAT);
    return LDB_ERR_FMT_DAT;
}

/**
 * pre-conditions:
 *   - obj->dat_fp != NULL
 *   - obj->idx_fp == NULL
 *   - obj->first_seqnum = set (0 if no data)
 *   - obj->first_timestamp = set
 *   - obj->format = set
 *
 * post-conditions (OK)
 *   - obj->dat_fp = set
 *   - obj->idx_fp = set
 *   - obj->first_seqnum = set (0 if no data)
 *   - obj->first_timestamp = set
 *   - obj->last_seqnum = set (0 if no data)
 *   - obj->last_timestamp = set
 *   - obj->dat_end = set
 * 
 * post-conditions (KO)
 *   - obj->idx_fp = NULL
 */
static int ldb_open_file_idx(ldb_db_t *obj, bool check)
{
    assert(obj);
    assert(obj->idx_fp == NULL);
    assert(obj->dat_fp != NULL);
    assert(!ferror(obj->dat_fp));

    int ret = LDB_OK;
    ldb_header_idx_t header = {0};
    ldb_record_idx_t record_0 = {0};
    ldb_record_idx_t record_n = {0};
    size_t pos = 0;
    size_t len = 0;

    obj->idx_fp = fopen(obj->idx_path, "r+");

    if (obj->idx_fp == NULL) {
        ldb_set_error(obj, "Cannot open file '%s': %s", obj->idx_path, strerror(errno));
        return LDB_ERR_FILE;
    }

    len = ldb_get_file_size(obj->idx_fp);

    if (fread(&header, sizeof(ldb_header_idx_t), 1, obj->idx_fp) != 1)
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

    pos += sizeof(ldb_header_idx_t);

    if (header.magic_number != LDB_MAGIC_NUMBER)
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

    if (header.format != LDB_FORMAT_1) {
        ldb_set_error(obj, "Unrecognized database format (idx)");
        ret = LDB_ERR_FMT_IDX;
        goto LDB_OPEN_FILE_IDX_ERR;
    }

    if (header.format != obj->format) {
        ldb_set_error(obj, "Database format mismatch (idx)");
        ret = LDB_ERR_FMT_IDX;
        goto LDB_OPEN_FILE_IDX_ERR;
    }

    if (pos + sizeof(ldb_record_idx_t) <= len)
    {
        // read first entry
        if (fread(&record_0, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1) 
            goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

        pos += sizeof(ldb_record_idx_t);

        if (record_0.seqnum != obj->first_seqnum)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

        if (record_0.timestamp != obj->first_timestamp)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

        if (record_0.seqnum != 0 && record_0.pos != sizeof(ldb_header_dat_t))
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;
    }

    record_n = record_0;

    // read last record distinct than 0
    if (record_0.seqnum == 0)
    {
        // do nothing
    }
    else if (check)
    {
        ldb_record_idx_t aux = {0};
        ldb_record_dat_t record_dat = {0};

        while (pos + sizeof(ldb_record_idx_t) <= len)
        {
            if (fread(&aux, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1) 
                goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

            if (ldb_is_record_idx_empty(&aux))
                break;

            pos += sizeof(ldb_record_idx_t);

            if (aux.seqnum != record_n.seqnum + 1 || aux.timestamp < record_n.timestamp || aux.pos < record_n.pos + sizeof(ldb_record_dat_t))
                goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

            if (fseek(obj->dat_fp, (long) aux.pos, SEEK_SET) != 0)
                goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

            if (fread(&record_dat, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1) 
                goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

            if (aux.seqnum != record_dat.seqnum || aux.timestamp != record_dat.timestamp)
                goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

            record_n = aux;
        }
    }
    else
    {
        // search last valid position
        long rem = (len - sizeof(ldb_header_idx_t)) % sizeof(ldb_record_idx_t);
        if (fseek(obj->idx_fp, -rem, SEEK_END) != 0)
            goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

        pos = (size_t)(len) - (size_t)(rem);

        // move backard until last record distinct than 0 (not rollbacked)
        while (pos > sizeof(ldb_header_idx_t))
        {
            if (fseek(obj->idx_fp, pos - sizeof(ldb_record_idx_t), SEEK_SET) != 0)
                goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

            if (fread(&record_n, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1) 
                goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

            if (!ldb_is_record_idx_empty(&record_n))
                break;

            pos -= sizeof(ldb_record_idx_t);
        }
    }

    // at this point pos is just after the last record distinct than 0
    if (fseek(obj->idx_fp, (long) pos, SEEK_SET) != 0)
        goto LDB_OPEN_FILE_IDX_ERR_READ_IDX;

    if (!ldb_is_rollbacked(obj->idx_fp))
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

    // case idx with no records
    if (record_0.seqnum == 0)
    {
        // adding first dat record (if any)
        if (obj->first_seqnum != 0)
        {
            record_0.seqnum = obj->first_seqnum;
            record_0.timestamp = obj->first_timestamp;
            record_0.pos = sizeof(ldb_header_dat_t);

            obj->last_seqnum = obj->first_seqnum;
            obj->last_timestamp = obj->first_timestamp;

            if ((ret = ldb_append_record_idx(obj, &record_0)) != LDB_OK)
                goto LDB_OPEN_FILE_IDX_ERR;

            record_n = record_0;
        }
    }
    else
    {
        // check record_n content
        size_t diff = record_n.seqnum - record_0.seqnum;

        if (record_n.seqnum < record_0.seqnum || record_n.timestamp < record_0.timestamp)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

        if (pos != sizeof(ldb_header_idx_t) + (diff + 1) * sizeof(ldb_record_idx_t))
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

        if (record_n.pos < sizeof(ldb_header_dat_t) + (diff + 1) * sizeof(ldb_record_dat_t))
            goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

        obj->last_seqnum = record_n.seqnum;
        obj->last_timestamp = record_n.timestamp;
    }

    // case no data
    if (obj->first_seqnum == 0) {
        obj->dat_end = sizeof(ldb_header_dat_t);
        return LDB_OK;
    }

    // read last record (dat)
    ldb_record_dat_t record_dat = {0};

    pos = record_n.pos;
    len = ldb_get_file_size(obj->dat_fp);

    if (fseek(obj->dat_fp, (long) pos, SEEK_SET) != 0)
        goto LDB_OPEN_FILE_IDX_ERR_READ_DAT;

    if (pos + sizeof(ldb_record_dat_t) > len)
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

    if (fread(&record_dat, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1) 
        goto LDB_OPEN_FILE_IDX_ERR_READ_DAT;

    pos += sizeof(ldb_record_dat_t);

    if (record_dat.seqnum != record_n.seqnum || record_dat.timestamp != record_n.timestamp)
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX; 

    pos += record_dat.metadata_len + record_dat.data_len;

    if (pos > len)
        goto LDB_OPEN_FILE_IDX_ERR_FMT_IDX;

    if (fseek(obj->dat_fp, (long) pos, SEEK_SET) != 0)
        goto LDB_OPEN_FILE_IDX_ERR_READ_DAT;

    obj->dat_end = pos;

    // add unflushed dat records (if any)
    while (pos + sizeof(ldb_record_dat_t) <= len)
    {
        if (fread(&record_dat, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1) 
            goto LDB_OPEN_FILE_IDX_ERR_READ_DAT;

        pos += sizeof(ldb_record_dat_t);
        
        if (ldb_is_record_dat_empty(&record_dat))
            break;

        if (record_dat.seqnum != obj->last_seqnum + 1)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_DAT;

        if (record_dat.timestamp < obj->last_timestamp)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_DAT;

        pos += record_dat.metadata_len + record_dat.data_len;

        if (pos > len)
            goto LDB_OPEN_FILE_IDX_ERR_FMT_DAT;

        if (fseek(obj->dat_fp, (long) pos, SEEK_SET) != 0)
            goto LDB_OPEN_FILE_IDX_ERR_READ_DAT;

        record_n.seqnum = record_dat.seqnum;
        record_n.timestamp = record_dat.timestamp;
        record_n.pos += obj->dat_end;

        obj->last_seqnum = record_dat.seqnum;
        obj->last_timestamp = record_dat.timestamp;
        obj->dat_end = (size_t) ftell(obj->dat_fp);

        if ((ret = ldb_append_record_idx(obj, &record_n)) != LDB_OK)
            goto LDB_OPEN_FILE_IDX_ERR;

        assert(obj->dat_end == pos);
    }

    return LDB_OK;

LDB_OPEN_FILE_IDX_ERR:
    ldb_close_file_idx(obj);
    return ret;

LDB_OPEN_FILE_IDX_ERR_READ_IDX:
    ldb_close_file_idx(obj);
    ldb_set_error(obj, "Error reading file %s%s", obj->name, LDB_EXT_IDX);
    return LDB_ERR_READ_IDX;

LDB_OPEN_FILE_IDX_ERR_FMT_IDX:
    ldb_close_file_idx(obj);
    ldb_set_error(obj, "Corrupted file: %s%s", obj->name, LDB_EXT_IDX);
    return LDB_ERR_FMT_IDX;

LDB_OPEN_FILE_IDX_ERR_READ_DAT:
    ldb_close_file_idx(obj);
    ldb_set_error(obj, "Error reading file %s%s", obj->name, LDB_EXT_DAT);
    return LDB_ERR_READ_IDX;

LDB_OPEN_FILE_IDX_ERR_FMT_DAT:
    ldb_close_file_idx(obj);
    ldb_set_error(obj, "Corrupted file: %s%s", obj->name, LDB_EXT_DAT);
    return LDB_ERR_FMT_IDX;
}

const char * ldb_version(void)
{
    static char version_str[32] = {0};

    if (version_str[0] == 0) {
        snprintf(version_str, sizeof(version_str), "%d.%d.%d", 
                LDB_VERSION_MAJOR, LDB_VERSION_MINOR, LDB_VERSION_PATCH);
    }

    return version_str;
}

int ldb_open(const char *path, const char *name, ldb_db_t *obj, bool check)
{
    if (path == NULL || name == NULL || obj == NULL)
        return LDB_ERR_ARG;

    int ret = LDB_OK;

    if (!ldb_is_valid_path(path)) {
        ldb_set_error(obj, "Cannot acces folder '%s'", path);
        ret = LDB_ERR_PATH;
        goto LDB_OPEN_ERR;
    }

    if (!ldb_is_valid_name(name)) {
        ldb_set_error(obj, "Invalid db name '%s'", name);
        ret = LDB_ERR_NAME;
        goto LDB_OPEN_ERR;
    }

    memset(obj, 0x00, sizeof(ldb_db_t));

    obj->name = strdup(name);
    obj->path = strdup(path);
    obj->dat_path = ldb_create_filename(path, name, LDB_EXT_DAT);
    obj->idx_path = ldb_create_filename(path, name, LDB_EXT_IDX);
    obj->dat_end = sizeof(ldb_header_dat_t);

    if (!obj->name || !obj->path || !obj->dat_path || !obj->idx_path) {
        ret = LDB_ERR_MEM;
        goto LDB_OPEN_ERR;
    }

    // case dat file not exist
    if (access(obj->dat_path, F_OK) != 0)
    {
        remove(obj->idx_path);

        if (!ldb_create_file_dat(obj->dat_path)) {
            ldb_set_error(obj, "Cannot create file '%s'", obj->dat_path);
            ret = LDB_ERR_FILE;
            goto LDB_OPEN_ERR;
        }
    }

    // case dat file not exist
    if (access(obj->idx_path, F_OK) != 0)
    {
        if (!ldb_create_file_idx(obj->idx_path)) {
            ldb_set_error(obj, "Cannot create file '%s'", obj->idx_path);
            ret = LDB_ERR_FILE;
            goto LDB_OPEN_ERR;
        }
    }

    if ((ret = ldb_open_file_dat(obj, check)) != LDB_OK)
        goto LDB_OPEN_ERR;

    ret = ldb_open_file_idx(obj, check);
    if (ret == LDB_ERR_READ_IDX || ret == LDB_ERR_FMT_IDX)
    {
        // try to rebuild the index file
        LDB_FREE(obj->error);
        remove(obj->idx_path);

        if (!ldb_create_file_idx(obj->idx_path)) {
            ldb_set_error(obj, "Cannot create file '%s'", obj->idx_path);
            ret = LDB_ERR_FILE;
            goto LDB_OPEN_ERR;
        }

        if ((ret = ldb_open_file_idx(obj, check)) != LDB_OK)
            goto LDB_OPEN_ERR;
    }

    assert(!feof(obj->dat_fp));
    assert(!feof(obj->idx_fp));
    assert(!ferror(obj->dat_fp));
    assert(!ferror(obj->idx_fp));

    return LDB_OK;

LDB_OPEN_ERR:
    ldb_free_db(obj);
    return ret;
}

int ldb_close(ldb_db_t *obj)
{
    return ldb_free_db(obj);
}

int ldb_append(ldb_db_t *obj, ldb_entry_t *entries, size_t len, size_t *num)
{
    if (!obj || !entries)
        return LDB_ERR_ARG;

    if (!obj->dat_fp || feof(obj->dat_fp) || ferror(obj->dat_fp))
        return LDB_ERR;

    if (!obj->idx_fp || feof(obj->idx_fp) || ferror(obj->idx_fp))
        return LDB_ERR;

    if (num != NULL)
        *num = 0;

    size_t i;
    int ret = LDB_OK;

    for (i = 0; i < len; i++)
    {
        if (entries[i].seqnum == 0)
            entries[i].seqnum = obj->last_seqnum + 1;

        if (entries[i].timestamp == 0) 
            entries[i].timestamp = LDB_MAX(ldb_get_millis(), obj->last_timestamp);

        ldb_record_idx_t record_idx = {
            .seqnum = entries[i].seqnum,
            .timestamp = entries[i].timestamp,
            .pos = obj->dat_end
        };

        if ((ret = ldb_append_entry_dat(obj, &entries[i])) != LDB_OK)
            break;

        if ((ret = ldb_append_record_idx(obj, &record_idx)) != LDB_OK)
            break;

        if (num != NULL)
            (*num)++;
    }

    if (i > 0 && fflush(obj->dat_fp) != 0)
        ret = LDB_ERR_WRITE_DAT;

    return ret;
}

int ldb_read(ldb_db_t *obj, uint64_t seqnum, ldb_entry_t *entries, size_t len, size_t *num)
{
    if (!obj || !entries)
        return LDB_ERR_ARG;

    if (!obj->dat_fp || feof(obj->dat_fp) || ferror(obj->dat_fp))
        return LDB_ERR;

    if (!obj->idx_fp || feof(obj->idx_fp) || ferror(obj->idx_fp))
        return LDB_ERR;

    if (num != NULL)
        *num = 0;

    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
        entries[i].timestamp = 0;
    }

    if (seqnum < obj->first_seqnum)
        seqnum = obj->first_seqnum;

    if (seqnum == 0 || seqnum < obj->first_seqnum || seqnum > obj->last_seqnum)
        return LDB_OK;

    int rc = 0;
    ldb_record_idx_t record_idx = {0};

    for (size_t i = 0; i < len && seqnum <= obj->last_seqnum; i++)
    {
        if ((rc = ldb_read_record_idx(obj, seqnum, &record_idx)) != LDB_OK)
            return rc;

        if ((rc = ldb_read_entry_dat(obj, record_idx.pos, entries + i)) != LDB_OK)
            return rc;

        if (entries[i].seqnum != seqnum)
            return LDB_ERR;

        seqnum++;

        if (num != NULL)
            (*num)++;
    }

    return LDB_OK;
}

int ldb_stats(ldb_db_t *obj, uint64_t seqnum1, uint64_t seqnum2, ldb_stats_t *stats)
{
    if (!obj || seqnum2 < seqnum1 || stats == NULL)
        return LDB_ERR_ARG;

    memset(stats, 0x00, sizeof(ldb_stats_t));

    if (obj->first_seqnum == 0) {
        return LDB_OK;
    }

    if (!obj->dat_fp || feof(obj->dat_fp) || ferror(obj->dat_fp))
        return LDB_ERR;

    if (!obj->idx_fp || feof(obj->idx_fp) || ferror(obj->idx_fp))
        return LDB_ERR;

    seqnum1 = ldb_clamp(seqnum1, obj->first_seqnum, obj->last_seqnum);
    seqnum2 = ldb_clamp(seqnum2, obj->first_seqnum, obj->last_seqnum);

    int rc = 0;
    ldb_record_idx_t record1 = {0};
    ldb_record_idx_t record2 = {0};
    ldb_record_dat_t record_dat = {0};

    if ((rc = ldb_read_record_idx(obj, seqnum1, &record1)) != LDB_OK)
        return rc;

    if ((rc = ldb_read_record_idx(obj, seqnum2, &record2)) != LDB_OK)
        return rc;

    if (record2.pos < record1.pos + (record2.seqnum - record1.seqnum) * sizeof(ldb_record_dat_t))
        return LDB_ERR;

    if ((rc = ldb_read_record_dat(obj, record2.pos, &record_dat)) != LDB_OK)
        return rc;

    if (record_dat.seqnum != seqnum2)
        return LDB_ERR;

    stats->min_seqnum = record1.seqnum;
    stats->min_timestamp = record1.timestamp;
    stats->max_seqnum = record2.seqnum;
    stats->max_timestamp = record2.timestamp;
    stats->num_entries = seqnum2 - seqnum1 + 1;
    stats->index_size = sizeof(ldb_record_idx_t) * stats->num_entries;
    stats->data_size = record2.pos - record1.pos + sizeof(ldb_record_dat_t) +
                       record_dat.metadata_len + record_dat.data_len;

    return LDB_OK;
}

// LDB_SEARCH_LOWER : Returns the seqnum of the first entry having timestamp not less than the given value.
// LDB_SEARCH_UPPER : Returns the seqnum of the first entry having timestamp greater than the given value.
int ldb_search_by_ts(ldb_db_t *obj, uint64_t timestamp, ldb_search_e mode, uint64_t *seqnum)
{
    if (!obj || !seqnum)
        return LDB_ERR_ARG;

    if (mode != LDB_SEARCH_LOWER && mode != LDB_SEARCH_UPPER)
        return LDB_ERR_ARG;

    if (!obj->dat_fp || feof(obj->dat_fp) || ferror(obj->dat_fp))
        return LDB_ERR;

    if (!obj->idx_fp || feof(obj->idx_fp) || ferror(obj->idx_fp))
        return LDB_ERR;

    *seqnum = 0;

    if (obj->first_seqnum == 0)
        return LDB_ERR_NOT_FOUND;

    if (mode == LDB_SEARCH_LOWER && obj->last_timestamp < timestamp)
        return LDB_ERR_NOT_FOUND;

    if (mode == LDB_SEARCH_UPPER && obj->last_timestamp <= timestamp)
        return LDB_ERR_NOT_FOUND;

    if (mode == LDB_SEARCH_LOWER && timestamp <= obj->first_timestamp) {
        *seqnum = obj->first_seqnum;
        return LDB_OK;
    }

    if (mode == LDB_SEARCH_UPPER && timestamp < obj->first_timestamp) {
        *seqnum = obj->first_seqnum;
        return LDB_OK;
    }

    int rc = 0;
    ldb_record_idx_t record = {0};
    uint64_t sn1 = obj->first_seqnum;
    uint64_t sn2 = obj->last_seqnum;
    uint64_t ts1 = obj->first_timestamp;
    uint64_t ts2 = obj->last_timestamp;

    assert(ts1 <= timestamp && timestamp <= ts2);

    while (sn1 + 1 < sn2 && ts1 != ts2)
    {
        uint64_t sn = (sn1 + sn2) / 2;

        if ((rc = ldb_read_record_idx(obj, sn, &record)) != LDB_OK)
            return rc;

        uint64_t ts = record.timestamp;

        if (ts < timestamp) {
            sn1 = sn;
            ts1 = ts;
        }
        else if (timestamp < ts || mode == LDB_SEARCH_LOWER) {
            sn2 = sn;
            ts2 = ts;
        }
        else {
            sn1 = sn;
            ts1 = ts;
        }
    }

    *seqnum = sn2;

    return LDB_OK;
}

#undef LDB_FREE
#undef LDB_MIN
#undef LDB_MAX

#endif /* LDB_IMPL */
