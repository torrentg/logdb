/*
MIT License

journal -- A simple log-structured library.
<https://github.com/torrentg/journal>

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

#ifndef LDB_JOURNAL_H
#define LDB_JOURNAL_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * A simple log-structured library.
 * 
 * Journal is essentially an append-only data file (*.dat) with an index file (*.idx) used to speed up lookups.
 * No complex data structures, no sophisticated algorithms, only basic file access.
 * We rely on the filesystem cache (managed by the operating system) to ensure read performance.
 * 
 * Main features:
 *   - Variable length record type
 *   - Records uniquely identified by a sequential number (seqnum)
 *   - There are no other indexes other than seqnum
 *   - Records can be appended and read by seqnum
 *   - Records cannot be updated or deleted
 *   - Allows reverting the last entries (rollback)
 *   - Supports read-write concurrency (multi-thread)
 *   - Automatic data recovery in case of catastrophic events
 *   - Minimal memory footprint
 *   - No dependencies
 * 
 * dat file format
 * ---------------
 * 
 * Contains the journal entries.
 * 
 * @see struct ldb_header_dat_t
 * @see struct ldb_record_dat_t
 * 
 *     header        record1          data1          record2       data2
 * ┌──────┴──────┐┌─────┴─────┐┌────────┴────────┐┌─────┴─────┐┌─────┴─────┐...
 *   magic number    seqnum1       raw bytes 1       seqnum2    raw bytes 2
 *      format       length1                         length2
 *     metadata      chksum1                         chksum2
 * 
 * idx file format
 * ---------------
 * 
 * Used to search for journal entries.
 * If idx file does not exist, it is rebuilt from the data.
 * 
 * @see struct ldb_header_idx_t
 * @see struct ldb_record_idx_t
 * 
 *      header      record1       record2
 * ┌──────┴──────┐┌─────┴─────┐┌─────┴─────┐...
 *   magic number    offset1      offset2
 *      format
 *      seqnum1
 * 
 * We can access directly any record by seqnum because:
 *  - we know the first seqnum in the file
 *  - we know the last seqnum in the file
 *  - idx header has fixed size
 *  - all idx records have same size
 *
 * We rely on the system file caches to store data in memory.
 * 
 * Concurrency
 * ---------------
 * 
 * Main goal: append() function not blocked.
 * File write ops are done with [dat|idx]_fp.
 * File read ops are done with [dat|idx]_fd.
 * 
 * We use 2 mutex:
 *   - data mutex: Ensures data integrity ([min|max]_seqnum)
 *                 Reduced scope (variables update)
 *   - file mutex: Ensures no reads are done during destructive writes
 *                 Extended scope (function execution)
 * 
 *                             File     Data
 * Thread        Function      Mutex    Mutex   Notes
 * -------------------------------------------------------------------
 *               ┌ open()         -       -     Initialize mutexes, create FILEs used to write and fds used to read
 *               ├ append()       -       W     dat and idx files flushed at the end. State updated after flush.
 * thread-write: ┼ rollback()     W       W
 *               ├ check()        W       W
 *               └ close()        -       -     Destroy mutexes, close files
 *               ┌ range()        R       R
 * thread-read:  ┼ read()         R       R
 */

#define LDB_VERSION_MAJOR          1
#define LDB_VERSION_MINOR          2
#define LDB_VERSION_PATCH          0

#define LDB_OK                     0
#define LDB_ERR                   -1
#define LDB_ERR_ARG               -2
#define LDB_ERR_MEM               -3
#define LDB_ERR_PATH              -4
#define LDB_ERR_NAME              -5
#define LDB_ERR_READONLY          -6
#define LDB_ERR_NOFILE_DAT        -7
#define LDB_ERR_NOFILE_IDX        -8
#define LDB_ERR_INVL_DAT          -9
#define LDB_ERR_INVL_IDX         -10
#define LDB_ERR_CORRUPT_DAT      -11
#define LDB_ERR_CORRUPT_IDX      -12
#define LDB_ERR_CREATE_DAT       -13
#define LDB_ERR_CREATE_IDX       -14
#define LDB_ERR_OPEN_DAT         -15
#define LDB_ERR_OPEN_IDX         -16
#define LDB_ERR_READ_DAT         -17
#define LDB_ERR_READ_IDX         -18
#define LDB_ERR_WRITE_DAT        -19
#define LDB_ERR_WRITE_IDX        -20
#define LDB_ERR_SEQNUM           -21
#define LDB_ERR_NODATA           -22
#define LDB_ERR_CHECKSUM         -23
#define LDB_ERR_NOT_FOUND        -24
#define LDB_ERR_LOCK             -25

#define LDB_OPEN_CREATE          (1 << 0)   // Create journal if it does not exist (default: false)
#define LDB_OPEN_READONLY        (1 << 1)   // Open journal in read-only mode (default: false)
#define LDB_OPEN_FSYNC           (1 << 2)   // Enable fsync after each write (default: false)

#define LDB_DAT_MAGIC_NUMBER       0x74616478656C706EULL
#define LDB_IDX_MAGIC_NUMBER       0x78646978656C706EULL
#define LDB_FILE_FORMAT            3
#define LDB_METADATA_LEN          64

#ifdef __cplusplus
extern "C" {
#endif

struct ldb_impl_t;
typedef struct ldb_impl_t ldb_journal_t;

typedef struct ldb_entry_t {
    uint64_t seqnum;              // Sequence number (0 = system assigned).
    uint32_t data_len;            // Length of data (in bytes).
    void *data;                   // Pointer to data.
} ldb_entry_t;

typedef struct ldb_range_t {
    uint64_t min_seqnum;          // Minimum sequence number (0 means no entries).
    uint64_t max_seqnum;          // Maximum sequence number (0 means no entries).
} ldb_range_t;

/**
 * Returns ldb library version.
 * @return Library version (semantic version, ex. 1.0.4).
 */
const char * ldb_version(void);

/**
 * Returns the textual description of the ldb error code.
 * 
 * @param[in] errnum Code error.
 * 
 * @return Textual description.
 */
const char * ldb_strerror(int errnum);

/**
 * Allocates a new ldb_journal_t (opaque) object.
 * 
 * @return Allocated object or NULL if no memory.
 */
ldb_journal_t * ldb_alloc(void);

/**
 * Deallocates a ldb_journal_t object.
 * 
 * @param[in] obj Object to deallocate.
 */
void ldb_free(ldb_journal_t *obj);

/**
 * Opens a journal.
 * 
 * Creates the journal files (dat+idx) if they do not exist (flag LDB_OPEN_CREATE).
 * Updates the index file if incomplete (not flushed + crash).
 * Rebuilds the index file when corrupted or not found.
 * 
 * @param[in,out] obj Uninitialized the journal object.
 * @param[in] path Directory where journal files are located.
 * @param[in] name Journal name (allowed characters: [a-zA-Z0-9_], max length = 32).
 * @param[in] flags Open flags (0, LDB_OPEN_CREATE, LDB_OPEN_READONLY, LDB_OPEN_FSYNC,
 *                  or combination of them).
 * 
 * @return Error code (0 = OK). On error, the journal is closed properly (ldb_close not required).
 *         You can check the errno value to get additional error details.
 */
int ldb_open(ldb_journal_t *obj, const char *path, const char *name, int flags);

/**
 * Closes a journal.
 * 
 * Closes open files and releases allocated memory.
 * 
 * @param[in,out] obj Journal to close (can be NULL).
 * 
 * @return Return code (0 = OK).
 */
int ldb_close(ldb_journal_t *obj);

/**
 * Access to journal metadata.
 * 
 * Metadata is a tiny user-defined content used to store additional information about the
 * journal content (ex. format of the saved entries). The metadata is stored in the journal 
 * file header and can be retrieved later using ldb_get_meta().
 * 
 * @param[in] obj Journal to modify.
 * @param[in,out] meta Metadata content (cannot exceed LDB_METADATA_LEN bytes).
 * @param[in] len Length of the metadata string.
 * 
 * @return Error code (0 = OK).
 */
int ldb_set_meta(ldb_journal_t *obj, const char *meta, size_t len);
int ldb_get_meta(ldb_journal_t *obj, char *meta, size_t len);

/**
 * Returns the range of sequence numbers available in the journal.
 * 
 * @param[in] obj Journal to use.
 * 
 * @return The range of sequence numbers available in the journal, or
 *         {0, 0} if no entries are available, or
 *         {UINT64_MAX, UINT64_MAX} if the journal is invalid.
 */
ldb_range_t ldb_get_range(ldb_journal_t *obj);

/**
 * Appends entries to the journal.
 * 
 * Entries are identified by their seqnum. 
 * First entry can have any seqnum distinct from 0.
 * The rest of the entries must have consecutive values (no gaps).
 * 
 * This function is not 'atomic'. Entries are appended sequentially. 
 * On error (ex. disk full) written entries are flushed and remaining entries
 * are reported as not written (see num return argument).
 * 
 * Seqnum values:
 *   - equals to 0 -> system assigns the sequential value.
 *   - distinct from 0 -> system checks that it is the next value.
 * 
 * File operations:
 *   - Data file is updated and flushed.
 *   - Index file is updated but not flushed.
 * 
 * Memory pointed to by entries is not modified and can be deallocated after the function call.
 * 
 * @param[in] obj Journal to modify.
 * @param[in,out] entries Entries to append to the journal. Memory pointed 
 *                  to by each entry is not modified. Seqnum are updated if 
 *                  they have value 0. 
 * @param[in] len Number of entries to append.
 * @param[out] num Number of entries appended (can be NULL).
 * 
 * @return Error code (0 = OK).
 */
int ldb_append(ldb_journal_t *obj, ldb_entry_t *entries, size_t len, size_t *num);

/**
 * Reads num entries starting from seqnum (included).
 * 
 * This function uses the pre-allocated buffer to bulk the disk data as-is,
 * significantly reducing the number of system calls and improving performance.
 * When the buffer size does not suffices you will need to handle the case
 * (see below).
 * 
 * Returned data entries (entries[x].data) are aligned to sizeof(uintptr_t)
 * as long as buffer is aligned to sizeof(uintptr_t).
 * 
 * On success:
 *   - Returns LDB_OK
 *   - num param contains the number of entries read
 *   - entries param contains the read entries
 *   - if num = len, all requested data was read
 *   - otherwise (num < len)
 *     - if entries[num].seqnum == 0 => last record reached
 *     - if entries[num].seqnum != 0 => not enough memory in buffer
 *          entries[num] is filled correctly but data pointer is NULL
 *          If the current buffer size is great than entries[num].data_len you can
 *          call ldb_read(obj, entries[num].seqnum, entries, len - num, ...) again.
 *          Otherwise you need to reallocate the buffer with at least entries[num].data_len + 32 bytes.
 *   - unused entries are signaled with seqnum = 0
 * 
 * @param[in] obj Journal to use.
 * @param[in] seqnum Initial sequence number.
 * @param[out] entries Array of uninitialized entries (min length = len).
 * @param[in] len Number of entries to read.
 * @param[out] buf Allocated buffer memory where data entries are copied.
 * @param[in] buf_len Length of the buffer memory (value great than 24).
 * @param[out] num Number of entries read (can be NULL).
 *                 If num is less than 'len' means that the last record was 
 *                 reached or that buffer was exhausted. Unused entries 
 *                 are signaled with seqnum = 0.
 * 
 * @return Error code (0 = OK).
 */
int ldb_read(ldb_journal_t *obj, uint64_t seqnum, ldb_entry_t *entries, size_t len, char *buf, size_t buf_len, size_t *num);

/**
 * Removes all entries greater than seqnum.
 * 
 * File operations:
 *   - Index file is updated (zeroed top-to-bottom) and flushed.
 *   - Data file is updated (zeroed bottom-to-top) and flushed.
 * 
 * @param[in] obj Journal to update.
 * @param[in] seqnum Sequence number from which records are removed (seqnum=0 removes all content).
 * 
 * @return Number of removed entries, or error if negative.
 */
long ldb_rollback(ldb_journal_t *obj, uint64_t seqnum);

/**
 * Checks (and optionally repairs) the integrity of a journal.
 *
 * If repair is true, the journal must not be open in read-only mode.
 *
 *   Data file issue                              Repairable
 *   -------------------------------------------  ----------
 *   Checksum mismatch in a record (+)            Yes (zeroed)
 *   Non-consecutive sequence numbers             Yes (zeroed)
 *   Trailing data after last valid record        Yes (zeroed)
 * 
 * (+) The checksum covers both the record header and its data. 
 *     If it does not match, the record is deemed invalid and 
 *     everything from that point onward is zeroed out.
 * 
 *   Index file issue                             Repairable
 *   -------------------------------------------  ----------
 *   Index seqnum mismatch with dat               Yes (rebuilt)
 *   Sequence gap in index records                Yes (rebuilt)
 *   Index entry position out of bounds           Yes (rebuilt)
 *   Missing index records                        Yes (rebuilt)
 *   Trailing data after last valid record        Yes (zeroed)
 *
 * @param[in] obj    Open journal to check.
 * @param[in] repair If true, attempt to repair detected issues.
 *                   Requires journal not opened in read-only mode.
 *
 * @return LDB_OK if the journal is consistent (or was successfully repaired),
 *         LDB_ERR_ARG if obj is NULL,
 *         LDB_ERR_READONLY if repair is true but journal is read-only,
 *         otherwise an error code.
 */
int ldb_check(ldb_journal_t *obj, bool repair);

/**
 * Splits a journal into two journals at the given sequence number.
 *
 * The source journal is opened with an exclusive lock and must already exist.
 * Journal A receives entries [seqnum1 .. seqnum-1] and journal B receives
 * entries [seqnum .. seqnum2]. Both output journals inherit the header
 * (including metadata) of the source journal. Index files are not generated;
 * they will be rebuilt automatically on the first ldb_open().
 *
 * The caller is responsible for ensuring:
 *   - seqnum is strictly inside the range (min_seqnum < seqnum <= max_seqnum)
 *   - name_a and name_b are valid journal names and do not already exist
 *   - name_a and name_b fit within the maximum name length
 *
 * If creation of journal B fails after journal A has been created,
 * journal A is removed before returning the error.
 *
 * Side effect: if the source index file is missing, it is rebuilt in the
 * source directory before the split is performed.
 *
 * @param[in] path    Directory where source and output journals are located.
 * @param[in] name    Source journal name.
 * @param[in] seqnum  First sequence number that goes into journal B.
 * @param[in] name_a  Output journal name for the first half.
 * @param[in] name_b  Output journal name for the second half.
 *
 * @return LDB_OK on success, or an error code on failure.
 */
int ldb_split(const char *path, const char *name, uint64_t seqnum, const char *name_a, const char *name_b);

/**
 * Joins two consecutive journals into a new journal.
 *
 * Both source journals are opened with an exclusive lock and must already exist.
 * The output journal receives entries [name1.min_seqnum .. name2.max_seqnum] and
 * inherits the header (including metadata) of the first source journal (name1).
 * The index file of the output journal is generated during the operation.
 *
 * If one of the source journals is empty, the result is a copy of the other.
 * If both are empty, the result is an empty journal inheriting name1's header.
 *
 * The caller is responsible for ensuring:
 *   - name1, name2 and name are valid journal names, all distinct from each other
 *   - name does not already exist
 *   - name1 and name2 are in the same directory (path)
 *   - name2.min_seqnum == name1.max_seqnum + 1 (consecutiveness)
 *
 * On success, the source journals (name1 and name2) are removed.
 * On error, any partially created output files are removed and
 * the source journals are left intact.
 *
 * Side effect: if a source index file is missing, it is rebuilt before
 * the join is performed.
 *
 * @param[in] path   Directory where source and output journals are located.
 * @param[in] name1  First source journal name.
 * @param[in] name2  Second source journal name.
 * @param[in] name   Output journal name.
 *
 * @return LDB_OK on success, or an error code on failure.
 */
int ldb_join(const char *path, const char *name1, const char *name2, const char *name);

#ifdef __cplusplus
}

#include <stdexcept>
#include <filesystem>
#include <functional>

namespace ldb {

/**
 * Wrapper on journal code.
 * 
 * This class ensures that a journal is properly allocated, opened, and closed
 * using the RAII idiom. It prevents resource leaks by managing the lifecycle
 * of the journal resource.
 * 
 * The class is default-constructible, non-copyable and movable.
 */
class journal_t
{
  public:

    journal_t() = default;

    journal_t(const std::filesystem::path &path, const std::string &name, int flags = LDB_OPEN_CREATE)
    {
        m_journal = ldb_alloc();

        if (!m_journal)
            throw std::bad_alloc();

        int rc = ldb_open(m_journal, path.c_str(), name.c_str(), flags);

        if (rc != LDB_OK) {
            ldb_free(m_journal);
            throw std::runtime_error(ldb_strerror(rc));
        }
    }

    ~journal_t()
    {
        if (m_journal) {
            ldb_close(m_journal);
            ldb_free(m_journal);
            m_journal = nullptr;
        }
    }

    journal_t(const journal_t &) = delete;
    journal_t & operator=(const journal_t&) = delete;

    journal_t(journal_t&& other) noexcept : journal_t() {
        swap(*this, other);
    }

    journal_t & operator=(journal_t&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    friend void swap(journal_t& first, journal_t& second) noexcept {
        using std::swap;
        swap(first.m_journal, second.m_journal);
    }

    int set_meta(const char *meta, size_t len) {
        return ldb_set_meta(m_journal, meta, len);
    }

    int get_meta(char *meta, size_t len) {
        return ldb_get_meta(m_journal, meta, len);
    }

    std::pair<uint64_t, uint64_t> get_range() {
        ldb_range_t range = ldb_get_range(m_journal);
        return {range.min_seqnum, range.max_seqnum};
    }

    int append(ldb_entry_t *entries, size_t len, size_t *num) {
        return ldb_append(m_journal, entries, len, num);
    }

    int read(uint64_t seqnum, ldb_entry_t *entries, size_t len, char *buf, size_t buf_len, size_t *num) {
        return ldb_read(m_journal, seqnum, entries, len, buf, buf_len, num);
    }

    long rollback(uint64_t seqnum) {
        return ldb_rollback(m_journal, seqnum);
    }

    int check(bool repair = false) {
        return ldb_check(m_journal, repair);
    }

  private:

    ldb_journal_t *m_journal = nullptr;
};

inline int split(const std::filesystem::path &path, const std::string &name, uint64_t seqnum, const std::string &name_a, const std::string &name_b) {
    return ldb_split(path.c_str(), name.c_str(), seqnum, name_a.c_str(), name_b.c_str());
}

inline int join(const std::filesystem::path &path, const std::string &name1, const std::string &name2, const std::string &name) {
    return ldb_join(path.c_str(), name1.c_str(), name2.c_str(), name.c_str());
}

} // namespace ldb

#endif

#endif /* LDB_JOURNAL_H */
