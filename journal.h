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

#ifndef JOURNAL_H
#define JOURNAL_H

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
 *   - Allows removing obsolete entries (purge)
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
 *   magic number    seqnum1      seqnum2
 *      format       offset1      offset2
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
 *   - data mutex: Ensures data integrity ([first|last]_seqnum)
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
 *               ├ purge()        W       W     
 *               └ close()        -       -     Destroy mutexes, close files
 *               ┌ stats()        R       R     
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
#define LDB_ERR_ENTRY_SEQNUM     -21
#define LDB_ERR_ENTRY_DATA       -22
#define LDB_ERR_NOT_FOUND        -23
#define LDB_ERR_TMP_FILE         -24
#define LDB_ERR_CHECKSUM         -25
#define LDB_ERR_LOCK             -26

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

typedef struct ldb_stats_t {
    uint64_t min_seqnum;          // Minimum sequence number (0 means no entries).
    uint64_t max_seqnum;          // Maximum sequence number (0 means no entries).
} ldb_stats_t;

/**
 * Callback type used by ldb_check() to report each issue found or repair action taken.
 *
 * @param[in] msg  Human-readable description of the issue or repair action.
 * @param[in] data Opaque pointer passed through from ldb_check().
 */
typedef void (*ldb_check_cb)(const char *msg, void *data);

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
 * Creates the journal files (dat+idx) if they do not exist.
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
 * @param[in,out] obj Journal to close.
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
 * Return statistics between seqnum1 and seqnum2 (both included).
 * 
 * The requested range [seqnum1, seqnum2] is clamped to the intersection with
 * the available journal data [min_seqnum, max_seqnum]:
 *   - If the ranges do not intersect: returns LDB_ERR_NOT_FOUND (empty result).
 *   - If the ranges partially intersect: stats are computed for the clamped range.
 *   - If the ranges fully overlap: stats are computed for all requested data.
 * 
 * Examples: (assuming that the journal has entries [10, 100])
 *   - Request [0, UINT64_MAX] -> clamped to [10, 100]
 *   - Request [20, 90]        -> clamped to [20, 90]
 *   - Request [5, 50]         -> clamped to [10, 50]
 *   - Request [50, 200]       -> clamped to [50, 100]
 *   - Request [5, 9]          -> returns LDB_ERR_NOT_FOUND
 *   - Request [101, 200]      -> returns LDB_ERR_NOT_FOUND
 *
 * Examples: (assuming that the journal is empty)
 *   - Request [0, 3]          -> clamped to [0, 0]
 *   - Request [3, 5]          -> LDB_ERR_NOT_FOUND
 * 
 * @param[in] obj Journal to use.
 * @param[in] seqnum1 First sequence number.
 * @param[in] seqnum2 Second sequence number (greater than or equal to seqnum1).
 * @param[out] stats Uninitialized statistics.
 * 
 * @return Error code (0 = OK).
 */
int ldb_stats(ldb_journal_t *obj, uint64_t seqnum1, uint64_t seqnum2, ldb_stats_t *stats);

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
 * Remove all entries less than seqnum.
 * 
 * This function is expensive because it recreates the dat and idx files.
 * 
 * To prevent data loss in case of outage, we do:
 *   - A temporary data file is created.
 *   - Preserved records are copied from the dat file to the temporary file.
 *   - Temporary, dat and idx files are closed
 *   - The idx file is removed
 *   - The temporary file is renamed to dat
 *   - The dat file is opened
 *   - The idx file is rebuilt
 * 
 * @param[in] obj Journal to update.
 * @param[in] seqnum Sequence number up to which records are removed.
 * 
 * @return Number of removed entries, or error if negative.
 */
long ldb_purge(ldb_journal_t *obj, uint64_t seqnum);

/**
 * Checks (and optionally repairs) the integrity of a journal.
 *
 * If repair is false the journal is opened read-only (no lock is acquired). 
 * Otherwise the journal is opened in read-write mode with an exclusive lock.
 *
 * The callback cb (if not NULL) is invoked once for every issue detected and
 * once for every repair action taken, with a human-readable description.
 *
 *   Data file issue                              Repairable
 *   -------------------------------------------  ----------
 *   File not found or cannot be opened           No
 *   File locked by another process               Retry after closing external process
 *   Invalid header (magic number or format)      No
 *   Checksum mismatch in a record (+)            Yes (zeroed)
 *   Non-consecutive sequence numbers             No
 *   Trailing data after last valid record        Yes (zeroed)
 * 
 * (+) The checksum covers both the record header and its data. 
 *     If it does not match, the record is deemed invalid and 
 *     everything from that point onward is zeroed out.
 * 
 *   Index file issue                             Repairable
 *   -------------------------------------------  ----------
 *   File not found or cannot be opened           No
 *   File locked by another process               Retry after closing external process
 *   Invalid header (magic number or format)      No
 *   Sequence gap in index records                Yes (rebuilt)
 *   Index entry position out of bounds           Yes (rebuilt)
 *   Index seqnum mismatch with dat               Yes (rebuilt)
 *   Missing index records                        Yes (rebuilt)
 *   Trailing data after last valid record        Yes (zeroed)
 *
 * @param[in] path   Directory where journal files are located.
 * @param[in] name   Journal name.
 * @param[in] repair If true, attempt to repair detected issues.
 * @param[in] cb     Optional callback invoked for each issue/repair message.
 * @param[in] data   Opaque pointer forwarded to cb.
 *
 * @return LDB_OK if the journal is consistent (or was successfully repaired),
 *         LDB_ERR otherwise.
 */
int ldb_check(const char *path, const char *name, bool repair, ldb_check_cb cb, void *user_data);

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

    int append(ldb_entry_t *entries, size_t len, size_t *num) {
        return ldb_append(m_journal, entries, len, num);
    }

    int read(uint64_t seqnum, ldb_entry_t *entries, size_t len, char *buf, size_t buf_len, size_t *num) {
        return ldb_read(m_journal, seqnum, entries, len, buf, buf_len, num);
    }

    int stats(uint64_t seqnum1, uint64_t seqnum2, ldb_stats_t *stats) {
        return ldb_stats(m_journal, seqnum1, seqnum2, stats);
    }

    long rollback(uint64_t seqnum) {
        return ldb_rollback(m_journal, seqnum);
    }

    long purge(uint64_t seqnum) {
        return ldb_purge(m_journal, seqnum);
    }

    static int check(const std::filesystem::path &path, const std::string &name, bool repair, std::function<void(const char*)> cb = nullptr)
    {
        if (!cb)
            return ldb_check(path.c_str(), name.c_str(), repair, nullptr, nullptr);

        auto trampoline = [](const char *msg, void *data) noexcept {
            (*static_cast<std::function<void(const char*)>*>(data))(msg);
        };

        return ldb_check(path.c_str(), name.c_str(), repair, trampoline, &cb);
    }

  private:

    ldb_journal_t *m_journal = nullptr;
};

} // namespace ldb

#endif

#endif /* JOURNAL_H */
