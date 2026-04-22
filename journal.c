#include <time.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/file.h>
#include "journal.h"

/**
 * Rule of thumb:
 * 
 *   - We use FILE and fwrite() to write to files. 
 *     These methods are buffered and faster than write() system call.
 *   - We use file descriptor and read() to read from files.
 *     All read content was previosuly flushed by fwrite().
 *     We rely on the filesystem cache for buffering.
 */

#define LDB_EXT_DAT             ".dat"
#define LDB_EXT_IDX             ".idx"
#define LDB_EXT_TMP             ".tmp"
#define LDB_PATH_SEPARATOR      "/"
#define LDB_NAME_MAX_LENGTH     32

#define LDB_STR_HELPER(x)  #x
#define LDB_STR(x)         LDB_STR_HELPER(x)

#define exit_function(retval) do { ret = retval; goto END_FUNCTION; } while(0)

#if defined(__GNUC__) || defined(__clang__) || defined(__INTEL_LLVM_COMPILER) 
    #define LDB_INLINE  __attribute__((const)) __attribute__((always_inline)) inline
    #define LDB_PURE    __attribute__((pure))
    #define PACKED      __attribute__((__packed__))
#else
    #define LDB_INLINE  /**/
    #define LDB_PURE    /**/
    #define PACKED      /**/
#endif

#if defined __has_attribute
    #if __has_attribute(__fallthrough__)
        # define fallthrough   __attribute__((__fallthrough__))
    #endif
#endif
#ifndef fallthrough
    # define fallthrough   do {} while (0)  /* fallthrough */
#endif

typedef struct ldb_state_t {
    uint64_t seqnum1;             // First sequence number (0 means no entries).
    uint64_t timestamp1;          // Timestamp of the first entry.
    uint64_t seqnum2;             // Last sequence number (0 means no entries).
    uint64_t timestamp2;          // Timestamp of the last entry.
} ldb_state_t;

typedef struct ldb_impl_t
{
    // Fixed data (unchanged)
    char *name;                   // Journal name
    char *path;                   // Directory where files are located
    char *dat_path;               // Data filepath (path + filename)
    char *idx_path;               // Index filepath (path + filename)
    uint32_t format;              // File format
    bool force_fsync;             // Force fsync after flush
    bool read_only;               // Journal opened in read-only mode

    // Shared data (accessed by both threads)
    pthread_mutex_t mutex_state;  // Prevents race condition on state values
    pthread_mutex_t mutex_files;  // Preserve coherence between shared variable and file contents
    ldb_state_t state;            // First and last seqnums and timestamps
    FILE *dat_fp;                 // Data file pointer (used to write)
    FILE *idx_fp;                 // Index file pointer (used to write)

    // Thread-write variables
    char padding[64];             // Padding to avoid destructive interference between threads
    size_t dat_end;               // Last position on data file

} ldb_impl_t;

typedef struct PACKED ldb_header_dat_t {
    uint64_t magic_number;
    uint32_t format;
    uint32_t padding;
    char metadata[LDB_METADATA_LEN];
} ldb_header_dat_t;

typedef struct PACKED ldb_header_idx_t {
    uint64_t magic_number;
    uint32_t format;
    uint32_t padding;
} ldb_header_idx_t;

typedef struct PACKED ldb_record_dat_t {
    uint64_t seqnum;
    uint64_t timestamp;
    uint32_t data_len;
    uint32_t checksum;
} ldb_record_dat_t;

typedef struct PACKED ldb_record_idx_t {
    uint64_t seqnum;
    uint64_t timestamp;
    uint64_t pos;
} ldb_record_idx_t;

/* generated using the AUTODIN II polynomial
 *    x^32 + x^26 + x^23 + x^22 + x^16 +
 *    x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x^1 + 1
 */
static const uint32_t ldb_crctab[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
    0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
    0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
    0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
    0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
    0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
    0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
    0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
    0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
    0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
    0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
    0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
    0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
    0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
    0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
    0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
    0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
    0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
    0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
    0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
    0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
    0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
};

#define LDB_CRC(crc, ch)     (crc = (crc >> 8) ^ ldb_crctab[(crc ^ (ch)) & 0xff])

/**
 * Computes the crc32 checksum.
 * 
 * @see https://opensource.apple.com/source/file_cmds/file_cmds-188/cksum/crc32.c
 * 
 * @param bytes Bytes to digest.
 * @param len Bytes length.
 * @param checksum Previous checksum value (0 on startup).
 * @return Checksum value updated.
 */
uint32_t ldb_crc32(const char *bytes, size_t len, uint32_t checksum)
{
    if (bytes == NULL || len == 0)
        return checksum;

    checksum = ~checksum;

    for (size_t i = 0; i < len; i++)
        LDB_CRC(checksum, (unsigned char) bytes[i]);

    return ~checksum;
}

const char * ldb_strerror(int errnum)
{
    if (errnum > 0)
        return "Success";

    switch(errnum)
    {
        case LDB_OK: return "Success";
        case LDB_ERR: return "Generic error";
        case LDB_ERR_ARG: return "Invalid argument";
        case LDB_ERR_MEM: return "Out of memory";
        case LDB_ERR_PATH: return "Invalid directory";
        case LDB_ERR_NAME: return "Invalid journal name";
        case LDB_ERR_FILE_NOT_FOUND: return "File not found";
        case LDB_ERR_READONLY: return "Journal is read-only";
        case LDB_ERR_OPEN_DAT: return "Cannot open dat file";
        case LDB_ERR_READ_DAT: return "Error reading dat file";
        case LDB_ERR_WRITE_DAT: return "Error writing to dat file";
        case LDB_ERR_OPEN_IDX: return "Cannot open idx file";
        case LDB_ERR_READ_IDX: return "Error reading idx file";
        case LDB_ERR_WRITE_IDX: return "Error writing to idx file";
        case LDB_ERR_FMT_DAT: return "Invalid dat file";
        case LDB_ERR_FMT_IDX: return "Invalid idx file";
        case LDB_ERR_ENTRY_SEQNUM: return "Broken sequence";
        case LDB_ERR_ENTRY_TIMESTAMP: return "Invalid timestamp";
        case LDB_ERR_ENTRY_DATA: return "Data not found";
        case LDB_ERR_NOT_FOUND: return "No results";
        case LDB_ERR_TMP_FILE: return "Error creating temp file";
        case LDB_ERR_CHECKSUM: return "Checksum mismatch";
        case LDB_ERR_LOCK: return "Error locking file";
        default: return "Unknown error";
    }
}

static uint64_t ldb_get_millis(void)
{
    struct timespec  ts = {0};

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
        return 0;

    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

LDB_INLINE
static size_t ldb_min(size_t a, size_t b) {
    return (a < b ? a : b);
}

LDB_INLINE
static uint64_t ldb_max(uint64_t a, uint64_t b) {
    return (a > b ? a : b);
}

LDB_INLINE
static uint64_t ldb_clamp(uint64_t val, uint64_t lo, uint64_t hi) {
    return (val < lo ? lo : (hi < val ? hi : val));
}

LDB_INLINE 
static size_t ldb_padding(size_t value) {
    size_t round_up = (value + (sizeof(uintptr_t) - 1)) & ~(sizeof(uintptr_t) - 1);
    return round_up - value;
}

LDB_PURE
static bool ldb_is_valid_obj(ldb_impl_t *obj) {
    return (obj &&
            obj->dat_fp && !feof(obj->dat_fp) && !ferror(obj->dat_fp) &&
            obj->idx_fp && !feof(obj->idx_fp) && !ferror(obj->idx_fp));
}

static void ldb_reset_state(ldb_state_t *state) {
    if (state) {
        state->seqnum1 = 0;
        state->timestamp1 = 0;
        state->seqnum2 = 0;
        state->timestamp2 = 0;
    }
}

static int ldb_close_files(ldb_impl_t *obj)
{
    if (!obj)
        return LDB_OK;

    int ret = LDB_OK;

    if (obj->idx_fp != NULL)
    {
        int idx_fd = fileno(obj->idx_fp);

        if (!obj->read_only)
            flock(idx_fd, LOCK_UN);

        if (fclose(obj->idx_fp) != 0)
            ret = LDB_ERR_WRITE_IDX;

        obj->idx_fp = NULL;
    }

    if (obj->dat_fp != NULL)
    { 
        int dat_fd = fileno(obj->dat_fp);

        if (!obj->read_only)
            flock(dat_fd, LOCK_UN);

        if (fclose(obj->dat_fp) != 0)
            ret = LDB_ERR_WRITE_DAT;

        obj->dat_fp = NULL;
    }

    obj->dat_end = 0;

    return ret;
}

#define LDB_FREE(ptr) do { free(ptr); ptr = NULL; } while(0)

int ldb_close(ldb_impl_t *obj)
{
    if (obj == NULL)
        return LDB_OK;

    int ret = ldb_close_files(obj);

    ldb_reset_state(&obj->state);

    if (obj->name) {
        pthread_mutex_destroy(&obj->mutex_state);
        pthread_mutex_destroy(&obj->mutex_files);
    }

    LDB_FREE(obj->name);
    LDB_FREE(obj->path);
    LDB_FREE(obj->dat_path);
    LDB_FREE(obj->idx_path);

    return ret;
}

#undef LDB_FREE

static bool ldb_is_valid_path(const char *path)
{
    struct stat statbuf = {0};

    if (path == NULL)
        return false;

    if (*path == 0)  // case cwd (current working directory)
        return true;

    if (stat(path, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
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

    return (*ptr == 0 && ptr - name <= LDB_NAME_MAX_LENGTH);
}

static char * ldb_create_filename(const char *path, const char *name, const char *ext)
{
    if (path == NULL || name == NULL || ext == NULL || *name == 0)
        return NULL;

    size_t len = strlen(path);
    bool path_sep_required = (len > 0 && path[len -1] != LDB_PATH_SEPARATOR[0]);

    len = len + 1 + strlen(name) + strlen(ext) + 1;
    char *filepath = (char *) calloc(len, 1);

    if (filepath == NULL)
        return NULL;

    snprintf(filepath, len, "%s%s%s%s", 
             path,
             (path_sep_required ? LDB_PATH_SEPARATOR : ""),
             name,
             ext);

    return filepath;
}

// Create a new dat file
// Returns error if the file already exists
static bool ldb_create_file_dat(const char *path)
{
    assert(path);

    if (access(path, F_OK) == 0)
        return false;

    bool ret = true;

    FILE *fp = fopen(path, "wx");
    if (fp == NULL)
        return false;

    ldb_header_dat_t header = {
        .magic_number = LDB_DAT_MAGIC_NUMBER,
        .format = LDB_FILE_FORMAT,
        .padding = 0,
        .metadata = {0}
    };

    if (fwrite(&header, sizeof(ldb_header_dat_t), 1, fp) != 1)
        ret = false;

    if (fclose(fp) != 0)
        ret = false;

    return ret;
}

// Create a new idx file
// Returns error if the file already exists
static bool ldb_create_file_idx(const char *path)
{
    assert(path);

    if (access(path, F_OK) == 0)
        return false;

    bool ret = true;

    FILE *fp = fopen(path, "wx");
    if (fp == NULL)
        return false;

    ldb_header_idx_t header = {
        .magic_number = LDB_IDX_MAGIC_NUMBER,
        .format = LDB_FILE_FORMAT,
        .padding = 0
    };

    if (fwrite(&header, sizeof(ldb_header_idx_t), 1, fp) != 1)
        ret = false;

    if (fclose(fp) != 0)
        ret = false;

    return ret;
}

// Returns file size preserving current file position
// Returns 0 on error
static size_t ldb_get_file_size(FILE *fp)
{
    if (fp == NULL)
        return 0;

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

// Set zero's from pos until the end of the file.
// At return file position is set at pos.
// On error return false, otherwise returns true.
static bool ldb_zeroize(FILE *fp, size_t pos)
{
    assert(fp);
    assert(!feof(fp));
    assert(!ferror(fp));

    const char buf[BUFSIZ] = {0};
    size_t max_pos = ldb_get_file_size(fp);

    if (max_pos < pos)
        return false;

    if (fseek(fp, (long) pos, SEEK_SET) != 0)
        return false;

    if (max_pos == pos)
        return true;

    for (size_t cur_pos = pos; cur_pos < max_pos; cur_pos += sizeof(buf))
        if (fwrite(buf, ldb_min(max_pos - cur_pos, sizeof(buf)), 1, fp) != 1)
            return false;

    if (fflush(fp) != 0)
        return false;

    if (fseek(fp, (long) pos, SEEK_SET) != 0)
        return false;

    assert(!feof(fp) && !ferror(fp));
    return true;
}

// Copy file1 content in range [pos0,pos1] to file2 at pos2.
// Preserve current file positions.
// Flush destination file.
// On error return false, otherwise returns true.
static bool ldb_copy_file(FILE *fp1, size_t pos0, size_t pos1, FILE *fp2, size_t pos2)
{
    assert(fp1);
    assert(!feof(fp1));
    assert(!ferror(fp1));
    assert(fp2);
    assert(!feof(fp2));
    assert(!ferror(fp2));
    assert(pos0 <= pos1);

    bool ret = false;
    char buf[BUFSIZ] = {0};
    long orig1 = ftell(fp1);
    long orig2 = ftell(fp2);
    size_t len1 = ldb_get_file_size(fp1);
    size_t len2 = ldb_get_file_size(fp2);

    if (orig1 < 0 || orig2 < 0)
        return false;

    if (pos0 > pos1 || pos1 > len1 || pos2 > len2)
        return false;

    if (pos0 == pos1)
        return true;

    if (fseek(fp1, (long) pos0, SEEK_SET) != 0)
        goto LDB_COPY_FILE_END;

    if (fseek(fp2, (long) pos2, SEEK_SET) != 0)
        goto LDB_COPY_FILE_END;

    for (size_t pos = pos0; pos < pos1; pos += sizeof(buf))
    {
        size_t num_bytes = ldb_min(pos1 - pos, sizeof(buf));

        if (fread(buf, 1, num_bytes, fp1) != num_bytes)
            goto LDB_COPY_FILE_END;

        if (fwrite(buf, num_bytes, 1, fp2) != 1)
            goto LDB_COPY_FILE_END;
    }

    assert(!feof(fp1) && !ferror(fp1));
    assert(!feof(fp2) && !ferror(fp2));

    ret = true;

LDB_COPY_FILE_END:
    fseek(fp1, orig1, SEEK_SET);
    fseek(fp2, orig2, SEEK_SET);
    return ret;
}

// returns the position in the idx file for a given seqnum
static size_t ldb_get_pos_idx(ldb_state_t *state, uint64_t seqnum)
{
    assert(state);
    assert(state->seqnum1 <= seqnum);

    size_t diff = (state->seqnum1 == 0 ? 0 : seqnum - state->seqnum1);
    return sizeof(ldb_header_idx_t) + diff * sizeof(ldb_record_idx_t);
}

static uint32_t ldb_checksum_record(ldb_record_dat_t *record)
{
    uint32_t checksum = 0;

    checksum = ldb_crc32((const char *) &record->seqnum, sizeof(record->seqnum), checksum);
    checksum = ldb_crc32((const char *) &record->timestamp, sizeof(record->timestamp), checksum);
    checksum = ldb_crc32((const char *) &record->data_len, sizeof(record->data_len), checksum);

    // required call to complete the checksum
    // call checksum = crc32(data, checksum)

    return checksum;
}

static uint32_t ldb_checksum_entry(ldb_entry_t *entry)
{
    uint32_t checksum = 0;

    checksum = ldb_crc32((const char *) &entry->seqnum, sizeof(entry->seqnum), checksum);
    checksum = ldb_crc32((const char *) &entry->timestamp, sizeof(entry->timestamp), checksum);
    checksum = ldb_crc32((const char *) &entry->data_len, sizeof(entry->data_len), checksum);

    if (entry->data_len && entry->data)
        checksum = ldb_crc32(entry->data, entry->data_len, checksum);

    return checksum;
}

// append data entry at position obj->dat_end
// updates obj->dat_end value
// function accessed only by thread-write
static int ldb_append_entry_dat(ldb_impl_t *obj, ldb_state_t *state, ldb_entry_t *entry)
{
    assert(obj);
    assert(state);
    assert(entry);
    assert(obj->dat_fp);
    assert(!feof(obj->dat_fp));
    assert(!ferror(obj->dat_fp));

    static const char zeros[sizeof(uintptr_t)] = {0};

    if (entry->data_len != 0 && entry->data == NULL)
        return LDB_ERR_ENTRY_DATA;

    if (state->seqnum2 != 0 && entry->seqnum != state->seqnum2 + 1)
        return LDB_ERR_ENTRY_SEQNUM;

    if (entry->timestamp < state->timestamp2)
        return LDB_ERR_ENTRY_TIMESTAMP;

    ldb_record_dat_t record = {
        .seqnum = entry->seqnum,
        .timestamp = entry->timestamp,
        .data_len = entry->data_len,
        .checksum = ldb_checksum_entry(entry)
    };

    if (fseek(obj->dat_fp, (long) obj->dat_end, SEEK_SET) != 0)
        return LDB_ERR_WRITE_DAT;

    if (fwrite(&record, sizeof(ldb_record_dat_t), 1, obj->dat_fp) != 1)
        return LDB_ERR_WRITE_DAT;

    if (record.data_len)
    {
        if (fwrite(entry->data, 1, record.data_len, obj->dat_fp) != record.data_len)
            return LDB_ERR_WRITE_DAT;

        size_t padding = ldb_padding(entry->data_len);

        if (fwrite(zeros, 1, padding, obj->dat_fp) != padding)
            return LDB_ERR_WRITE_DAT;
    }

    if (state->seqnum1 == 0) {
        state->seqnum1 = entry->seqnum;
        state->timestamp1 = entry->timestamp;
    }

    state->seqnum2 = entry->seqnum;
    state->timestamp2 = entry->timestamp;

    long pos = ftell(obj->dat_fp);
    if (pos < 0)
        return LDB_ERR_READ_DAT;

    obj->dat_end = (size_t) pos;

    return LDB_OK;
}

static int ldb_append_record_idx(ldb_impl_t *obj, ldb_state_t *state, ldb_record_idx_t *record)
{
    assert(obj);
    assert(state);
    assert(record);
    assert(obj->idx_fp);
    assert(!feof(obj->idx_fp));
    assert(!ferror(obj->idx_fp));

    if (record->seqnum != state->seqnum2) {
        return LDB_ERR;
    }

    size_t pos = ldb_get_pos_idx(state, record->seqnum);

    if (fseek(obj->idx_fp, (long) pos, SEEK_SET) != 0)
        return LDB_ERR_READ_IDX;

    if (fwrite(record, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1)
        return LDB_ERR_WRITE_IDX;

    return LDB_OK;
}

// Read data record at pos.
// File position is not modified.
static int ldb_read_record_dat(int fd, size_t pos, ldb_record_dat_t *record, bool verify_checksum)
{
    assert(record);
    assert(fd > STDERR_FILENO);

    ssize_t rc = pread(fd, record, sizeof(ldb_record_dat_t), (off_t) pos);

    if (rc == -1)
        return LDB_ERR_READ_DAT;

    if (rc != sizeof(ldb_record_dat_t))
        return LDB_ERR_FMT_DAT;

    if (!verify_checksum || record->seqnum == 0)
        return LDB_OK;

    uint32_t checksum = ldb_checksum_record(record);
    size_t len = record->data_len;

    if (len > 0)
    {
        pos += sizeof(ldb_record_dat_t);

        char buf[BUFSIZ] = {0};
        size_t end = pos + len;

        while (pos < end)
        {
            size_t num_bytes = ldb_min(end - pos, sizeof(buf));

            rc = pread(fd, buf, num_bytes, (off_t) pos);

            if (rc == -1)
                return LDB_ERR_READ_DAT;

            if (rc != (ssize_t) num_bytes)
                return LDB_ERR_FMT_DAT;

            pos += num_bytes;

            checksum = ldb_crc32(buf, num_bytes, checksum);
        }
    }

    if (checksum != record->checksum)
        return LDB_ERR_CHECKSUM;

    return LDB_OK;
}

// Read idx record at pos.
// File position is not modified.
static int ldb_read_record_idx(int fd, ldb_state_t *state, uint64_t seqnum, ldb_record_idx_t *record)
{
    assert(state);
    assert(record);
    assert(fd > STDERR_FILENO);

    if (state->seqnum1 == 0 || seqnum < state->seqnum1 || state->seqnum2 < seqnum)
        return LDB_ERR;

    if (seqnum == state->seqnum1) {
        record->seqnum = state->seqnum1;
        record->timestamp = state->timestamp1;
        record->pos = sizeof(ldb_header_dat_t);
        return LDB_OK;
    }

    size_t pos = ldb_get_pos_idx(state, seqnum);

    if (pread(fd, record, sizeof(ldb_record_idx_t), (off_t) pos) != (ssize_t) sizeof(ldb_record_idx_t))
        return LDB_ERR_READ_IDX;

    if (record->seqnum != seqnum)
        return LDB_ERR;

    return LDB_OK;
}

/**
 * Opens the dat file and reads the first record.
 * No validation of remaining records, no repair.
 *
 * post-conditions (OK)
 *   - obj->dat_fp = set (file position at end)
 *   - obj->format = set
 *   - obj->state.seqnum1 = set (0 if no data)
 *   - obj->state.timestamp1 = set (0 if no data)
 *
 * post-conditions (KO)
 *   - dat file closed
 */
static int ldb_open_file_dat(ldb_impl_t *obj)
{
    assert(obj);
    assert(obj->dat_fp == NULL);
    assert(obj->idx_fp == NULL);

    int ret = LDB_OK;
    int dat_fd = -1;
    ldb_header_dat_t header = {0};
    ldb_record_dat_t record = {0};
    size_t pos = 0;
    size_t len = 0;

    ldb_reset_state(&obj->state);
    obj->dat_end = sizeof(ldb_header_dat_t);

    obj->dat_fp = fopen(obj->dat_path, obj->read_only ? "r" : "r+");

    if (obj->dat_fp == NULL)
        return LDB_ERR_OPEN_DAT;

    if ((dat_fd = fileno(obj->dat_fp)) == -1)
        exit_function(LDB_ERR_OPEN_DAT);

    if (!obj->read_only && flock(dat_fd, LOCK_EX | LOCK_NB) == -1)
        exit_function(LDB_ERR_LOCK);

    len = ldb_get_file_size(obj->dat_fp);

    if (pread(dat_fd, &header, sizeof(ldb_header_dat_t), 0) != (ssize_t) sizeof(ldb_header_dat_t))
        exit_function(LDB_ERR_FMT_DAT);

    pos += sizeof(ldb_header_dat_t);

    if (header.magic_number != LDB_DAT_MAGIC_NUMBER) 
        exit_function(LDB_ERR_FMT_DAT);

    if (header.format != LDB_FILE_FORMAT)
        exit_function(LDB_ERR_FMT_DAT);

    obj->format = header.format;

    // read first entry (if any)
    if (pos + sizeof(ldb_record_dat_t) <= len)
    {
        ret = ldb_read_record_dat(dat_fd, pos, &record, true);

        if (ret == LDB_OK && record.seqnum != 0)
        {
            obj->state.seqnum1 = record.seqnum;
            obj->state.timestamp1 = record.timestamp;
        }
    }

    if (fseek(obj->dat_fp, 0, SEEK_END) == -1)
        exit_function(LDB_ERR_READ_DAT);

    return LDB_OK;

END_FUNCTION:
    ldb_close_files(obj);
    ldb_reset_state(&obj->state);
    return ret;
}

/**
 * Opens the idx file and determines journal state from existing idx records.
 * Uses backwards search to find the last valid idx record.
 * No full validation, no repair, no writes.
 *
 * post-conditions (OK)
 *   - obj->idx_fp = set (file position at end)
 *   - obj->state.seqnum2 = set (0 if no data or idx empty)
 *   - obj->state.timestamp2 = set
 *   - obj->dat_end = set
 *
 * post-conditions (KO)
 *   - idx file closed
 */
static int ldb_open_file_idx(ldb_impl_t *obj)
{
    assert(obj);
    assert(obj->idx_fp == NULL);
    assert(obj->dat_fp != NULL);
    assert(!ferror(obj->dat_fp) && !feof(obj->dat_fp));

    int ret = LDB_OK;
    int dat_fd = -1;
    int idx_fd = -1;
    ldb_header_idx_t header = {0};
    ldb_record_idx_t record_0 = {0};
    ldb_record_idx_t record_n = {0};
    ldb_record_dat_t record_dat = {0};
    size_t pos = 0;
    size_t len = 0;

    if ((dat_fd = fileno(obj->dat_fp)) == -1)
        return LDB_ERR_OPEN_DAT;

    obj->idx_fp = fopen(obj->idx_path, obj->read_only ? "r" : "r+");

    if (obj->idx_fp == NULL)
        return LDB_ERR_OPEN_IDX;

    if ((idx_fd = fileno(obj->idx_fp)) == -1)
        exit_function(LDB_ERR_OPEN_IDX);

    if (!obj->read_only && flock(idx_fd, LOCK_EX | LOCK_NB) == -1)
        exit_function(LDB_ERR_LOCK);

    len = ldb_get_file_size(obj->idx_fp);

    if (pread(idx_fd, &header, sizeof(ldb_header_idx_t), 0) != (ssize_t) sizeof(ldb_header_idx_t))
        exit_function(LDB_ERR_FMT_IDX);

    pos += sizeof(ldb_header_idx_t);

    if (header.magic_number != LDB_IDX_MAGIC_NUMBER)
        exit_function(LDB_ERR_FMT_IDX);

    if (header.format != LDB_FILE_FORMAT)
        exit_function(LDB_ERR_FMT_IDX);

    if (header.format != obj->format)
        exit_function(LDB_ERR_FMT_IDX);

    // read first idx record (if any)
    if (pos + sizeof(ldb_record_idx_t) <= len)
    {
        if (pread(idx_fd, &record_0, sizeof(ldb_record_idx_t), (off_t) pos) != (ssize_t) sizeof(ldb_record_idx_t)) 
            exit_function(LDB_ERR_READ_IDX);

        if (record_0.seqnum != obj->state.seqnum1)
            exit_function(LDB_ERR_FMT_IDX);

        if (record_0.timestamp != obj->state.timestamp1)
            exit_function(LDB_ERR_FMT_IDX);

        if (record_0.seqnum != 0 && record_0.pos != sizeof(ldb_header_dat_t))
            exit_function(LDB_ERR_FMT_IDX);
    }

    record_n = record_0;

    if (record_0.seqnum != 0)
    {
        // backwards search for last non-zero record
        long rem = ((long) len - (long) sizeof(ldb_header_idx_t)) % (int) sizeof(ldb_record_idx_t);
        pos = len - (size_t)(rem);

        while (pos > sizeof(ldb_header_idx_t))
        {
            off_t new_pos = (off_t)(pos - sizeof(ldb_record_idx_t));

            if (pread(idx_fd, &record_n, sizeof(ldb_record_idx_t), new_pos) != (ssize_t) sizeof(ldb_record_idx_t)) 
                exit_function(LDB_ERR_READ_IDX);

            if (record_n.seqnum != 0)
                break;

            pos -= sizeof(ldb_record_idx_t);
        }

        // validate last record sanity
        if (record_n.seqnum < record_0.seqnum || record_n.timestamp < record_0.timestamp)
            exit_function(LDB_ERR_FMT_IDX);

        // validate last record against dat
        if (ldb_read_record_dat(dat_fd, record_n.pos, &record_dat, true) != LDB_OK)
            exit_function(LDB_ERR_FMT_IDX);

        if (record_dat.seqnum != record_n.seqnum || record_dat.timestamp != record_n.timestamp)
            exit_function(LDB_ERR_FMT_IDX);

        obj->state.seqnum2 = record_n.seqnum;
        obj->state.timestamp2 = record_n.timestamp;
        obj->dat_end = record_n.pos + sizeof(ldb_record_dat_t) + record_dat.data_len + ldb_padding(record_dat.data_len);

        // verify no unindexed entries remain in dat after the last indexed entry
        if (ldb_read_record_dat(dat_fd, obj->dat_end, &record_dat, true) == LDB_OK) {
            if (record_dat.seqnum == obj->state.seqnum2 + 1)
                exit_function(LDB_ERR_FMT_IDX);
        }
    }

    // case no data
    if (obj->state.seqnum1 == 0) {
        obj->dat_end = sizeof(ldb_header_dat_t);
    }

    if (fseek(obj->idx_fp, 0, SEEK_END) == -1)
        exit_function(LDB_ERR_READ_IDX);

    return LDB_OK;

END_FUNCTION:
    if (obj->idx_fp != NULL) fclose(obj->idx_fp);
    obj->idx_fp = NULL;
    return ret;
}

/**
 * Rebuilds the idx file from the dat file contents.
 * Caller must hold mutex_files (or guarantee exclusive access).
 * On success: idx_fp is open (at end), state and dat_end are updated.
 * On error: idx_fp may be NULL, state may be inconsistent.
 *
 * pre-conditions:
 *   - obj->dat_fp = open and valid
 *
 * post-conditions (OK):
 *   - obj->idx_fp = set (file position at end)
 *   - obj->state = updated from dat scan
 *   - obj->dat_end = updated
 */
static int ldb_rebuild_idx(ldb_impl_t *obj)
{
    assert(obj);
    assert(obj->dat_fp != NULL);
    assert(!feof(obj->dat_fp) && !ferror(obj->dat_fp));

    int ret = LDB_OK;
    int dat_fd = fileno(obj->dat_fp);
    int idx_fd = -1;
    ldb_record_dat_t record_dat = {0};
    ldb_record_idx_t record_idx = {0};
    ldb_state_t new_state = {0};
    size_t pos = sizeof(ldb_header_dat_t);
    size_t dat_len = ldb_get_file_size(obj->dat_fp);
    size_t valid_end = sizeof(ldb_header_dat_t);

    // Step 1: Close and recreate idx
    if (obj->idx_fp != NULL)
    {
        fclose(obj->idx_fp);
        obj->idx_fp = NULL;
    }

    assert(obj->idx_path);
    remove(obj->idx_path);

    if (!ldb_create_file_idx(obj->idx_path))
        exit_function(LDB_ERR_OPEN_IDX);

    obj->idx_fp = fopen(obj->idx_path, "r+");

    if (obj->idx_fp == NULL)
        exit_function(LDB_ERR_OPEN_IDX);

    if ((idx_fd = fileno(obj->idx_fp)) == -1)
        exit_function(LDB_ERR_OPEN_IDX);

    if (flock(idx_fd, LOCK_EX | LOCK_NB) == -1)
        exit_function(LDB_ERR_LOCK);

    // Step 2: Scan dat and write idx in a single pass
    while (pos + sizeof(ldb_record_dat_t) <= dat_len)
    {
        ret = ldb_read_record_dat(dat_fd, pos, &record_dat, true);

        if (ret != LDB_OK)
            break;

        if (record_dat.seqnum == 0)
            break;

        if (new_state.seqnum2 != 0 && record_dat.seqnum != new_state.seqnum2 + 1)
            break;

        if (record_dat.timestamp < new_state.timestamp2)
            break;

        size_t rec_len = sizeof(ldb_record_dat_t) + record_dat.data_len + ldb_padding(record_dat.data_len);

        if (pos + rec_len > dat_len)
            break;

        if (new_state.seqnum1 == 0)
        {
            new_state.seqnum1 = record_dat.seqnum;
            new_state.timestamp1 = record_dat.timestamp;
        }

        new_state.seqnum2 = record_dat.seqnum;
        new_state.timestamp2 = record_dat.timestamp;

        // write idx record
        record_idx.seqnum = record_dat.seqnum;
        record_idx.timestamp = record_dat.timestamp;
        record_idx.pos = pos;

        if ((ret = ldb_append_record_idx(obj, &new_state, &record_idx)) != LDB_OK)
            exit_function(ret);

        pos += rec_len;
        valid_end = pos;
    }

    if (fflush(obj->idx_fp) != 0)
        exit_function(LDB_ERR_WRITE_IDX);

    // Step 3: Update state
    obj->state = new_state;
    obj->dat_end = valid_end;

    if (fseek(obj->dat_fp, 0, SEEK_END) == -1)
        exit_function(LDB_ERR_READ_DAT);

    if (fseek(obj->idx_fp, 0, SEEK_END) == -1)
        exit_function(LDB_ERR_READ_IDX);

    return LDB_OK;

END_FUNCTION:
    if (obj->idx_fp != NULL) {
        fclose(obj->idx_fp);
        obj->idx_fp = NULL;
    }
    return ret;
}

/**
 * Full validation of journal files (read-only).
 * 
 * Scans all data records verifying checksums, sequence numbers and timestamps.
 * Cross-checks every index record against the corresponding data record.
 * Reports the first error found.
 * 
 * Requires an open journal.
 * 
 * @param[in] obj Journal to validate.
 * 
 * @return Error code (0 = OK, journal is consistent).
 */
static int ldb_check(ldb_impl_t *obj)
{
    if (!obj)
        return LDB_ERR_ARG;

    if (!ldb_is_valid_obj(obj))
        return LDB_ERR;

    int ret = LDB_OK;
    int dat_fd = -1;
    int idx_fd = -1;
    ldb_state_t state;
    ldb_record_dat_t record_dat = {0};
    ldb_record_idx_t record_idx = {0};
    size_t dat_pos = sizeof(ldb_header_dat_t);
    size_t dat_len = 0;
    size_t idx_pos = sizeof(ldb_header_idx_t);
    size_t idx_len = 0;
    uint64_t prev_seqnum = 0;
    uint64_t prev_timestamp = 0;

    pthread_mutex_lock(&obj->mutex_files);

    dat_fd = fileno(obj->dat_fp);
    idx_fd = fileno(obj->idx_fp);

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    dat_len = ldb_get_file_size(obj->dat_fp);
    idx_len = ldb_get_file_size(obj->idx_fp);

    // empty journal
    if (state.seqnum1 == 0)
    {
        if (dat_len != sizeof(ldb_header_dat_t))
            exit_function(LDB_ERR_FMT_DAT);
        if (idx_len != sizeof(ldb_header_idx_t))
            exit_function(LDB_ERR_FMT_IDX);
        exit_function(LDB_OK);
    }

    // scan all dat records and cross-check with idx
    while (dat_pos + sizeof(ldb_record_dat_t) <= dat_len)
    {
        ret = ldb_read_record_dat(dat_fd, dat_pos, &record_dat, true);

        if (ret != LDB_OK)
            exit_function(ret);

        if (record_dat.seqnum == 0)
            break;

        // verify sequence
        if (prev_seqnum == 0)
        {
            if (record_dat.seqnum != state.seqnum1)
                exit_function(LDB_ERR_FMT_DAT);
        }
        else
        {
            if (record_dat.seqnum != prev_seqnum + 1)
                exit_function(LDB_ERR_FMT_DAT);
        }

        // verify timestamp monotonic
        if (record_dat.timestamp < prev_timestamp)
            exit_function(LDB_ERR_FMT_DAT);

        // cross-check with idx
        if (idx_pos + sizeof(ldb_record_idx_t) > idx_len)
            exit_function(LDB_ERR_FMT_IDX);

        if (pread(idx_fd, &record_idx, sizeof(ldb_record_idx_t), (off_t) idx_pos) != (ssize_t) sizeof(ldb_record_idx_t))
            exit_function(LDB_ERR_READ_IDX);

        if (record_idx.seqnum != record_dat.seqnum)
            exit_function(LDB_ERR_FMT_IDX);

        if (record_idx.timestamp != record_dat.timestamp)
            exit_function(LDB_ERR_FMT_IDX);

        if (record_idx.pos != dat_pos)
            exit_function(LDB_ERR_FMT_IDX);

        prev_seqnum = record_dat.seqnum;
        prev_timestamp = record_dat.timestamp;

        dat_pos += sizeof(ldb_record_dat_t) + record_dat.data_len + ldb_padding(record_dat.data_len);
        idx_pos += sizeof(ldb_record_idx_t);
    }

    // verify state matches
    if (prev_seqnum != state.seqnum2)
        exit_function(LDB_ERR_FMT_DAT);

    if (prev_timestamp != state.timestamp2)
        exit_function(LDB_ERR_FMT_DAT);

    // verify dat_end matches
    if (dat_pos != obj->dat_end)
        exit_function(LDB_ERR_FMT_DAT);

    // verify no extra idx records
    if (idx_pos != idx_len)
        exit_function(LDB_ERR_FMT_IDX);

    // verify no trailing dat data
    if (dat_pos != dat_len)
        exit_function(LDB_ERR_FMT_DAT);

    ret = LDB_OK;

END_FUNCTION:
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

/**
 * Repairs journal files.
 * 
 * Scans the data file to find all valid records, zeroizes trailing corruption,
 * and rebuilds the index file from scratch. Recovers data records that were
 * not flushed to the index (e.g. after a crash).
 * 
 * Requires an open journal.
 * 
 * @param[in] obj Journal to repair.
 * 
 * @return Error code (0 = OK).
 */
static int ldb_repair(ldb_impl_t *obj)
{
    if (!obj)
        return LDB_ERR_ARG;

    if (!obj->dat_fp || feof(obj->dat_fp) || ferror(obj->dat_fp))
        return LDB_ERR;

    int ret = LDB_OK;

    pthread_mutex_lock(&obj->mutex_files);

    // Zeroize trailing dat corruption (rebuild_idx determines valid_end)
    // We need to scan dat first to find valid_end, then zeroize, then rebuild.
    // Since ldb_rebuild_idx scans dat and determines valid_end,
    // we zeroize after rebuild using the computed dat_end.

    if ((ret = ldb_rebuild_idx(obj)) != LDB_OK)
        exit_function(ret);

    if (!ldb_zeroize(obj->dat_fp, obj->dat_end))
        exit_function(LDB_ERR_WRITE_DAT);

    if (fflush(obj->dat_fp) != 0)
        exit_function(LDB_ERR_WRITE_DAT);

    if (fseek(obj->dat_fp, 0, SEEK_END) == -1)
        exit_function(LDB_ERR_WRITE_DAT);

END_FUNCTION:

    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

const char * ldb_version(void)
{
    return LDB_STR(LDB_VERSION_MAJOR) "." LDB_STR(LDB_VERSION_MINOR) "." LDB_STR(LDB_VERSION_PATCH);
}

int ldb_open(ldb_impl_t *obj, const char *path, const char *name, int flags)
{
    if (path == NULL || name == NULL || obj == NULL)
        return LDB_ERR_ARG;

    if (!ldb_is_valid_path(path))
        return LDB_ERR_PATH;

    if (!ldb_is_valid_name(name))
        return  LDB_ERR_NAME;

    int ret = LDB_OK;

    memset(obj, 0x00, sizeof(ldb_impl_t));

    obj->name = strdup(name);
    obj->path = strdup(path);
    obj->dat_path = ldb_create_filename(path, name, LDB_EXT_DAT);
    obj->idx_path = ldb_create_filename(path, name, LDB_EXT_IDX);

    if (!obj->name || !obj->path || !obj->dat_path || !obj->idx_path)
        exit_function(LDB_ERR_MEM);

    obj->force_fsync = ((flags & LDB_OPEN_FSYNC) != 0);
    obj->read_only = ((flags & LDB_OPEN_READONLY) != 0);
    obj->dat_end = sizeof(ldb_header_dat_t);

    pthread_mutex_init(&obj->mutex_state, NULL);
    pthread_mutex_init(&obj->mutex_files, NULL);

    // case dat file not exist
    if (access(obj->dat_path, F_OK) != 0)
    {
        if (!(flags & LDB_OPEN_CREATE))
            exit_function(LDB_ERR_FILE_NOT_FOUND);

        if (flags & LDB_OPEN_READONLY)
            exit_function(LDB_ERR_READONLY);

        if (!ldb_create_file_dat(obj->dat_path))
            exit_function(LDB_ERR_OPEN_DAT);
    }

    // open data file
    if ((ret = ldb_open_file_dat(obj)) != LDB_OK)
        exit_function(ret);

    // open index file (may not exist)
    if ((ret = ldb_open_file_idx(obj)) != LDB_OK)
    {
        if (ret != LDB_ERR_OPEN_IDX && ret != LDB_ERR_READ_IDX && ret != LDB_ERR_FMT_IDX)
            exit_function(ret);

        if (flags & LDB_OPEN_READONLY)
            exit_function(LDB_ERR_READONLY);

        if ((ret = ldb_rebuild_idx(obj)) != LDB_OK)
            exit_function(ret);
    }

    // check data integrity
    if (flags & LDB_OPEN_CHECK)
    {
        if ((ret = ldb_check(obj)) != LDB_OK)
        {
            if (!(flags & LDB_OPEN_REPAIR))
                exit_function(ret);

            if (flags & LDB_OPEN_READONLY)
                exit_function(LDB_ERR_READONLY);

            if ((ret = ldb_repair(obj)) != LDB_OK)
                exit_function(ret);
        }
    }

    assert(!feof(obj->dat_fp));
    assert(!feof(obj->idx_fp));
    assert(!ferror(obj->dat_fp));
    assert(!ferror(obj->idx_fp));

    return LDB_OK;

END_FUNCTION:
    ldb_close(obj);
    return ret;
}

int ldb_append(ldb_impl_t *obj, ldb_entry_t *entries, size_t len, size_t *num)
{
    if (num != NULL)
        *num = 0;

    if (!obj || !entries)
        return LDB_ERR_ARG;

    if (obj->read_only)
        return LDB_ERR_READONLY;

    if (!ldb_is_valid_obj(obj))
        return LDB_ERR;

    if (len == 0)
        return LDB_OK;

    size_t i;
    int ret = LDB_OK;
    ldb_state_t state;

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    for (i = 0; i < len; i++)
    {
        if (entries[i].seqnum == 0)
            entries[i].seqnum = state.seqnum2 + 1;

        if (entries[i].timestamp == 0) 
            entries[i].timestamp = ldb_max(ldb_get_millis(), state.timestamp2);

        ldb_record_idx_t record_idx = {
            .seqnum = entries[i].seqnum,
            .timestamp = entries[i].timestamp,
            .pos = obj->dat_end
        };

        if ((ret = ldb_append_entry_dat(obj, &state, &entries[i])) != LDB_OK)
            break;

        if ((ret = ldb_append_record_idx(obj, &state, &record_idx)) != LDB_OK)
            break;

        if (num != NULL)
            (*num)++;
    }

    if (i == 0)
        return ret;

    if (fflush(obj->dat_fp) != 0)
        ret = (ret == LDB_OK ? LDB_ERR_WRITE_DAT : ret);

    if (fflush(obj->idx_fp) != 0)
        ret = (ret == LDB_OK ? LDB_ERR_WRITE_IDX : ret);

    if (obj->force_fsync && fdatasync(fileno(obj->dat_fp)) == -1)
        ret = (ret == LDB_OK ? LDB_ERR_WRITE_DAT : ret);

    pthread_mutex_lock(&obj->mutex_state);
    obj->state = state;
    pthread_mutex_unlock(&obj->mutex_state);

    return ret;
}

int ldb_read(ldb_journal_t *obj, uint64_t seqnum, ldb_entry_t *entries, size_t len, char *buf, size_t buf_len, size_t *num)
{
    if (num != NULL)
        *num = 0;

    if (!obj || !entries || len == 0 || !buf || buf_len < sizeof(ldb_record_dat_t))
        return LDB_ERR_ARG;

    for (size_t i = 0; i < len; i++) {
        entries[i].seqnum = 0;
        entries[i].timestamp = 0;
        entries[i].data_len = 0;
        entries[i].data = NULL;
    }

    pthread_mutex_lock(&obj->mutex_files);

    int ret = LDB_ERR;
    int dat_fd = -1;
    int idx_fd = -1;
    uint64_t read_pos = 0;
    uint64_t read_bytes = 0;
    ldb_state_t state = {0};
    ldb_record_idx_t record_idx = {0};
    const ldb_record_dat_t *record_dat_ptr = NULL;
    size_t padding = 0;
    ssize_t bytes = 0;
    uint64_t seq = 0;
    size_t idx = 0;

    if (!ldb_is_valid_obj(obj))
        exit_function(LDB_ERR);

    dat_fd = fileno(obj->dat_fp);
    idx_fd = fileno(obj->idx_fp);

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    if (seqnum == 0 || seqnum < state.seqnum1 || seqnum > state.seqnum2)
        exit_function(LDB_ERR_NOT_FOUND);

    if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum, &record_idx)) != LDB_OK)
        exit_function(ret);

    read_pos = record_idx.pos;

    if (seqnum + len <= state.seqnum2)
    {
        if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum + len, &record_idx)) != LDB_OK)
            exit_function(ret);

        assert(record_idx.pos > read_pos);
        read_bytes = ldb_min(record_idx.pos - read_pos, buf_len);
    }
    else
    {
        read_bytes = buf_len;
    }

    bytes = pread(dat_fd, buf, read_bytes, (off_t) read_pos);

    if (bytes < (ssize_t) sizeof(ldb_record_dat_t))
        exit_function(LDB_ERR_READ_DAT);

    seq = seqnum - 1;

    while (idx < len && seq < state.seqnum2)
    {
        if (bytes < (ssize_t) sizeof(ldb_record_dat_t))
        {
            // In this skewed case (buffer overflow and read() ending in the 
            // middle of a record), we invalidate the previous entry (that 
            // was succesfully read). This avoid us from doing an additional
            // reading and allows us to notify the caller that the buffer
            // is too small.

            assert(idx > 0);
            entries[idx - 1].data = NULL;
            idx--;
            break;
        }

        record_dat_ptr = (ldb_record_dat_t *) buf;

        assert(seq + 1 == record_dat_ptr->seqnum);

        entries[idx].seqnum = record_dat_ptr->seqnum;
        entries[idx].timestamp = record_dat_ptr->timestamp;
        entries[idx].data_len = record_dat_ptr->data_len;
        entries[idx].data = buf + sizeof(ldb_record_dat_t);

        assert(((uintptr_t) entries[idx].data) % sizeof(uintptr_t) == 0);

        buf += sizeof(ldb_record_dat_t);
        bytes -= (ssize_t) sizeof(ldb_record_dat_t);

        if (bytes < (ssize_t) record_dat_ptr->data_len) {
            entries[idx].data = NULL;
            break;
        }

        buf += record_dat_ptr->data_len;
        bytes -= record_dat_ptr->data_len;

        padding = ldb_min(ldb_padding(record_dat_ptr->data_len), (size_t) bytes);

        buf += padding;
        bytes -= (ssize_t) padding;

        seq = record_dat_ptr->seqnum;
        idx++;
    }

    if (num != NULL)
        *num = idx;

    // TODO: validate checksum?

    ret = LDB_OK;

END_FUNCTION:
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

int ldb_stats(ldb_impl_t *obj, uint64_t seqnum1, uint64_t seqnum2, ldb_stats_t *stats)
{
    if (!obj || seqnum2 < seqnum1 || !stats)
        return LDB_ERR_ARG;

    memset(stats, 0x00, sizeof(ldb_stats_t));

    pthread_mutex_lock(&obj->mutex_files);

    int ret = LDB_ERR;
    int dat_fd = -1;
    int idx_fd = -1;
    ldb_state_t state;
    ldb_record_idx_t record1 = {0};
    ldb_record_idx_t record2 = {0};
    ldb_record_dat_t record_dat = {0};

    if (!ldb_is_valid_obj(obj))
        exit_function(LDB_ERR);

    dat_fd = fileno(obj->dat_fp);
    idx_fd = fileno(obj->idx_fp);

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    if (state.seqnum1 == 0 || seqnum2 < state.seqnum1 || state.seqnum2 < seqnum1)
        exit_function(LDB_OK);

    seqnum1 = ldb_clamp(seqnum1, state.seqnum1, state.seqnum2);
    seqnum2 = ldb_clamp(seqnum2, state.seqnum1, state.seqnum2);

    if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum1, &record1)) != LDB_OK)
        exit_function(ret);

    if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum2, &record2)) != LDB_OK)
        exit_function(ret);

    if (record2.pos < record1.pos + (record2.seqnum - record1.seqnum) * sizeof(ldb_record_dat_t))
        exit_function(LDB_ERR);

    if ((ret = ldb_read_record_dat(dat_fd, record2.pos, &record_dat, false)) != LDB_OK)
        exit_function(ret);

    if (record_dat.seqnum != seqnum2)
        exit_function(LDB_ERR);

    stats->min_seqnum = record1.seqnum;
    stats->min_timestamp = record1.timestamp;
    stats->max_seqnum = record2.seqnum;
    stats->max_timestamp = record2.timestamp;
    stats->num_entries = seqnum2 - seqnum1 + 1;
    stats->index_size = sizeof(ldb_record_idx_t) * stats->num_entries;
    stats->data_size = record2.pos - record1.pos + sizeof(ldb_record_dat_t) + record_dat.data_len + ldb_padding(record_dat.data_len);

    ret = LDB_OK;

END_FUNCTION:
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

int ldb_search(ldb_impl_t *obj, uint64_t timestamp, ldb_search_e mode, uint64_t *seqnum)
{
    if (!obj || !seqnum || (mode != LDB_SEARCH_LOWER && mode != LDB_SEARCH_UPPER))
        return LDB_ERR_ARG;

    *seqnum = 0;

    pthread_mutex_lock(&obj->mutex_files);

    int ret = LDB_ERR;
    ldb_state_t state;
    ldb_record_idx_t record = {0};
    uint64_t sn1 = 0;
    uint64_t sn2 = 0;
    uint64_t ts1 = 0;
    uint64_t ts2 = 0;
    int idx_fd = -1;

    if (!ldb_is_valid_obj(obj))
        exit_function(LDB_ERR);

    idx_fd = fileno(obj->idx_fp);

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    if (state.seqnum1 == 0)
        exit_function(LDB_ERR_NOT_FOUND);

    if (mode == LDB_SEARCH_LOWER && state.timestamp2 < timestamp)
        exit_function(LDB_ERR_NOT_FOUND);

    if (mode == LDB_SEARCH_UPPER && state.timestamp2 <= timestamp)
        exit_function(LDB_ERR_NOT_FOUND);

    if (mode == LDB_SEARCH_LOWER && timestamp <= state.timestamp1) {
        *seqnum = state.seqnum1;
        exit_function(LDB_OK);
    }

    if (mode == LDB_SEARCH_UPPER && timestamp < state.timestamp1) {
        *seqnum = state.seqnum1;
        exit_function(LDB_OK);
    }

    sn1 = state.seqnum1;
    sn2 = state.seqnum2;
    ts1 = state.timestamp1;
    ts2 = state.timestamp2;

    assert(ts1 <= timestamp && timestamp <= ts2);

    while (sn1 + 1 < sn2 && ts1 != ts2)
    {
        uint64_t sn = (sn1 + sn2) / 2;

        if ((ret = ldb_read_record_idx(idx_fd, &state, sn, &record)) != LDB_OK)
            exit_function(ret);

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

    ret = LDB_OK;

END_FUNCTION:
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

long ldb_rollback(ldb_impl_t *obj, uint64_t seqnum)
{
    if (!obj)
        return LDB_ERR_ARG;

    if (obj->read_only)
        return LDB_ERR_READONLY;

    pthread_mutex_lock(&obj->mutex_files);

    long ret = LDB_ERR;
    long removed_entries = 0;
    ldb_state_t state = {0};
    ldb_record_idx_t record_idx = {0};
    size_t dat_end_new = sizeof(ldb_header_dat_t);
    uint64_t last_timestamp_new = 0;
    uint64_t csn = 0;
    int idx_fd = -1;

    if (!ldb_is_valid_obj(obj))
        exit_function(LDB_ERR);

    idx_fd = fileno(obj->idx_fp);

    pthread_mutex_lock(&obj->mutex_state);
    state = obj->state;
    pthread_mutex_unlock(&obj->mutex_state);

    // case nothing to rollback
    if (state.seqnum1 == 0 || state.seqnum2 <= seqnum)
        exit_function(0);

    removed_entries = (long) state.seqnum2 - (long) ldb_max(seqnum, state.seqnum1 - 1);
    csn = state.seqnum2;

    if (seqnum >= state.seqnum1)
    {
        if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum, &record_idx)) != LDB_OK)
            exit_function(ret);

        last_timestamp_new = record_idx.timestamp;

        if ((ret = ldb_read_record_idx(idx_fd, &state, seqnum + 1, &record_idx)) != LDB_OK)
            exit_function(ret);

        dat_end_new = record_idx.pos;
    }

    memset(&record_idx, 0x00, sizeof(ldb_record_idx_t));

    // set index entries to 0 (from top to down)
    while (seqnum < csn && state.seqnum1 <= csn)
    {
        size_t pos = ldb_get_pos_idx(&state, csn);

        if (fseek(obj->idx_fp, (long) pos, SEEK_SET) != 0)
            exit_function(LDB_ERR_READ_IDX);

        if (fwrite(&record_idx, sizeof(ldb_record_idx_t), 1, obj->idx_fp) != 1)
            exit_function(LDB_ERR_WRITE_IDX);

        csn--;
    }

    if (fflush(obj->idx_fp) != 0)
        exit_function(LDB_ERR_WRITE_IDX);

    // update status
    if (seqnum < state.seqnum1) {
        state.seqnum1 = 0;
        state.timestamp1 = 0;
        state.seqnum2 = 0;
        state.timestamp2 = 0;
        obj->dat_end = sizeof(ldb_header_dat_t);
    }
    else {
        state.seqnum2 = seqnum;
        state.timestamp2 = last_timestamp_new;
        obj->dat_end = dat_end_new;
    }

    pthread_mutex_lock(&obj->mutex_state);
    obj->state = state;
    pthread_mutex_unlock(&obj->mutex_state);

    // set data entries to 0 (from down to top)
    if (!ldb_zeroize(obj->dat_fp, dat_end_new))
        exit_function(LDB_ERR_WRITE_DAT);

    if (fflush(obj->dat_fp) != 0)
        exit_function(LDB_ERR_WRITE_DAT);

    if (obj->force_fsync && fdatasync(fileno(obj->dat_fp)) == -1)
        exit_function(LDB_ERR_WRITE_DAT);

    ret = removed_entries;

END_FUNCTION:
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

long ldb_purge(ldb_impl_t *obj, uint64_t seqnum)
{
    if (!obj)
        return LDB_ERR_ARG;

    if (obj->read_only)
        return LDB_ERR_READONLY;

    pthread_mutex_lock(&obj->mutex_files);
    pthread_mutex_lock(&obj->mutex_state);

    int ret = LDB_ERR;
    long removed_entries = 0;
    ldb_header_dat_t header_dat = {0};
    ldb_record_idx_t record_idx = {0};
    ldb_record_dat_t record_dat = {0};
    char *tmp_path = NULL;
    FILE *tmp_fp = NULL;
    int dat_fd = -1;
    int idx_fd = -1;
    size_t pos = 0;

    if (!ldb_is_valid_obj(obj))
        exit_function(LDB_ERR);

    // case no entries to purge
    if (seqnum <= obj->state.seqnum1 || obj->state.seqnum1 == 0) {
        pthread_mutex_unlock(&obj->mutex_state);
        pthread_mutex_unlock(&obj->mutex_files);
        return 0;
    }

    dat_fd = fileno(obj->dat_fp);
    idx_fd = fileno(obj->idx_fp);

    // case purge all entries
    if (obj->state.seqnum2 < seqnum)
    {
        removed_entries = (long) obj->state.seqnum2 - (long) obj->state.seqnum1 + 1;

        ldb_close_files(obj);
        ldb_reset_state(&obj->state);

        remove(obj->dat_path);
        remove(obj->idx_path);

        if (!ldb_create_file_dat(obj->dat_path))
            exit_function(LDB_ERR_OPEN_DAT);

        if ((ret = ldb_open_file_dat(obj)) != LDB_OK)
            exit_function(ret);

        if ((ret = ldb_rebuild_idx(obj)) != LDB_OK)
            exit_function(ret);

        pthread_mutex_unlock(&obj->mutex_state);
        pthread_mutex_unlock(&obj->mutex_files);

        return removed_entries;
    }

    // case purge some entries

    removed_entries = (long) seqnum - (long) obj->state.seqnum1;

    if (pread(dat_fd, &header_dat, sizeof(ldb_header_dat_t), 0) != (ssize_t) sizeof(ldb_header_dat_t))
        exit_function(LDB_ERR_READ_DAT);

    if ((ret = ldb_read_record_idx(idx_fd, &obj->state, seqnum, &record_idx)) != LDB_OK)
        exit_function(ret);

    pos = record_idx.pos;

    if ((ret = ldb_read_record_dat(dat_fd, pos, &record_dat, true)) != LDB_OK)
        exit_function(ret);

    if (record_dat.seqnum != seqnum)
        exit_function(LDB_ERR_FMT_IDX);

    if ((tmp_path = ldb_create_filename(obj->path, obj->name, LDB_EXT_TMP)) == NULL)
        exit_function(LDB_ERR_MEM);

    if ((tmp_fp = fopen(tmp_path, "w")) == NULL)
        exit_function(LDB_ERR_TMP_FILE);

    if (fwrite(&header_dat, sizeof(ldb_header_dat_t), 1, tmp_fp) != 1)
        exit_function(LDB_ERR_TMP_FILE);

    if (!ldb_copy_file(obj->dat_fp, pos, obj->dat_end, tmp_fp, sizeof(ldb_header_dat_t)))
        exit_function(LDB_ERR_TMP_FILE);

    if (fclose(tmp_fp) != 0)
        exit_function(LDB_ERR_TMP_FILE);

    tmp_fp = NULL;

    if ((ret = ldb_close_files(obj)) != LDB_OK)
        exit_function(ret);

    ldb_reset_state(&obj->state);

    remove(obj->idx_path);

    if (rename(tmp_path, obj->dat_path) != 0)
        exit_function(LDB_ERR_TMP_FILE);

    free(tmp_path);
    tmp_path = NULL;

    if ((ret = ldb_open_file_dat(obj)) != LDB_OK)
        exit_function(ret);

    if ((ret = ldb_rebuild_idx(obj)) != LDB_OK)
        exit_function(ret);

    pthread_mutex_unlock(&obj->mutex_state);
    pthread_mutex_unlock(&obj->mutex_files);

    return removed_entries;

END_FUNCTION:
    free(tmp_path);
    if (tmp_fp != NULL) fclose(tmp_fp);
    ldb_close_files(obj);
    ldb_reset_state(&obj->state);
    pthread_mutex_unlock(&obj->mutex_state);
    pthread_mutex_unlock(&obj->mutex_files);
    return ret;
}

ldb_journal_t * ldb_alloc(void) {
    return (ldb_journal_t *) calloc(1, sizeof(ldb_impl_t));
}

void ldb_free(ldb_journal_t *obj) {
    free(obj);
}

int ldb_set_meta(ldb_journal_t *obj, const char *meta, size_t len)
{
    static const char zero[LDB_METADATA_LEN] = {0};

    if (!obj || !meta || len > LDB_METADATA_LEN)
        return LDB_ERR_ARG;

    if (obj->read_only)
        return LDB_ERR_READONLY;

    if (!ldb_is_valid_obj(obj))
        return LDB_ERR;

    fflush(obj->dat_fp);

    int dat_fd = fileno(obj->dat_fp);
    off_t pos = offsetof(ldb_header_dat_t, metadata);

    if (pwrite(dat_fd, meta, len, pos) != (ssize_t) len)
        return LDB_ERR_WRITE_DAT;

    if (len < LDB_METADATA_LEN)
    {
        pos += (off_t) len;
        len = LDB_METADATA_LEN - len;

        if (pwrite(dat_fd, zero, len, pos) != (ssize_t) len)
            return LDB_ERR_WRITE_DAT;
    }

    return LDB_OK;
}

int ldb_get_meta(ldb_journal_t *obj, char *meta, size_t len)
{
    if (!obj || !meta || len > LDB_METADATA_LEN)
        return LDB_ERR_ARG;

    if (!ldb_is_valid_obj(obj))
        return LDB_ERR;

    int dat_fd = fileno(obj->dat_fp);
    off_t pos = offsetof(ldb_header_dat_t, metadata);

    if (pread(dat_fd, meta, len, pos) != (ssize_t) len)
        return LDB_ERR_READ_DAT;

    return LDB_OK;
}

#undef exit_function
