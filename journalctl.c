#define _POSIX_C_SOURCE 200809L

#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <ctype.h>
#include <assert.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <inttypes.h>
#include <sys/stat.h>
#include "journal.h"

#define APP_NAME                "journalctl"
#define DEFAULT_PATH            "."
#define EXT_DAT                 ".dat"
#define PATH_SEPARATOR          "/"
#define MAX_ENTRIES             128
#define BUF_GROWTH_FACTOR       2

#define MIN(a,b) (((a) < (b)) ? (a) : (b))

void print_journal_entry(FILE *out, const ldb_entry_t *entry);

// Hack to access internal journal info
typedef struct journal_header_t {
    char *name;
    char *path;
    char *dat_path;
    char *idx_path;
    uint32_t format;
} journal_header_t;

typedef enum mode_e {
    MODE_SUMMARY,
    MODE_BULK,
    MODE_REPAIR,
    MODE_PURGE,
    MODE_ROLLBACK,
} mode_e;

typedef struct params_t {
    mode_e mode;
    const char *path;
    const char *name;
    int flags;

    // bulk
    bool have_from;
    bool have_to;
    uint64_t from;
    uint64_t to;

    // purge/rollback
    bool have_num;
    bool have_seq;
    uint64_t num;
    uint64_t seq;
} params_t;

static void print_help(FILE *out)
{
    fprintf(out,
        "%s - journal maintenance tool\n"
        "\n"
        "Usage:\n"
        "  %s -h\n"
        "  %s --summary  [-p PATH] [-c] NAME\n"
        "  %s --bulk     [-p PATH] [-c] [-f NUM] [-t NUM] NAME\n"
        "  %s --repair   [-p PATH] NAME\n"
        "  %s --purge    [-p PATH] (-n NUM | -s SEQ) NAME\n"
        "  %s --rollback [-p PATH] (-n NUM | -s SEQ) NAME\n"
        "\n"
        "Options:\n"
        "      --summary           Print a summary for NAME (default mode)\n"
        "      --bulk              List entries in a seqnum range\n"
        "      --repair            Check and repair journal consistency\n"
        "      --purge             Remove oldest entries (from start)\n"
        "      --rollback          Remove newest entries (from end)\n"
        "  -h, --help              Show this help and exit\n"
        "  -p, --path=PATH         Directory containing NAME.dat/NAME.idx (default: .)\n"
        "  -c, --check             Validate journal consistency when opening\n"
        "  -f, --from=NUM          First seqnum (inclusive)\n"
        "  -t, --to=NUM            Last seqnum (inclusive)\n"
        "  -n, --num=NUM           Number of entries to remove\n"
        "  -s, --seq=SEQ           New boundary (purge keeps from SEQ; rollback keeps up to SEQ)\n"
        "\n"
        "Environment:\n"
        "  TZ                      Time zone used for displaying timestamps\n"
        "\n"
        "Exit codes:\n"
        "  0  Success\n"
        "  1  Failure (invalid args, missing files, locked files, I/O errors, etc.)\n",
        APP_NAME, APP_NAME, APP_NAME, APP_NAME, APP_NAME, APP_NAME, APP_NAME);
}

static bool parse_u64(const char *s, uint64_t *out)
{
    if (!s || !out)
        return false;

    while (isspace((int)*s))
        s++;

    if (!isdigit((int)*s))
        return false;

    errno = 0;

    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);

    if (errno != 0 || end == s || *end != '\0')
        return false;

    *out = (uint64_t)v;
    return true;
}

static bool format_timestamp(uint64_t timestamp, char *out, size_t out_len)
{
    time_t sec = (time_t)(timestamp / 1000);
    int ms = (int)(timestamp % 1000);
    struct tm tmv = {0};
    char suffix[16] = {0};
    char base[32] = {0};
    char zbuf[16] = {0};
    int rc = 0;

    if (!localtime_r(&sec, &tmv))
        return false;

    if (strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &tmv) == 0)
        return false;

    if (strftime(zbuf, sizeof(zbuf), "%z", &tmv) != 0)
    {
        if (strcmp(zbuf, "+0000") == 0 || strcmp(zbuf, "-0000") == 0)   // set 'Z' for UTC
            snprintf(suffix, sizeof(suffix), "Z");
        else if (strlen(zbuf) == 5)                                     // convert +hhmm to +hh:mm
            snprintf(suffix, sizeof(suffix), "%c%c%c:%c%c", zbuf[0], zbuf[1], zbuf[2], zbuf[3], zbuf[4]);
        else
            snprintf(suffix, sizeof(suffix), "%s", zbuf);
    }

    rc = snprintf(out, out_len, "%s.%03d%s", base, ms, suffix);

    return (rc > 0 && (size_t)rc < out_len);
}

static void print_hexdump(FILE *out, const unsigned char *p, size_t len)
{
    const size_t bytes_per_line = 16;
    bool wide_offset = (len > 0xFFFF);

    if (len == 0) {
        fprintf(out, "    <empty>\n");
        return;
    }

    for (size_t i = 0; i < len; i += bytes_per_line)
    {
        size_t n = (len - i > bytes_per_line ? bytes_per_line : (len - i));

        if (wide_offset)
            fprintf(out, "    %08zX: ", i);
        else
            fprintf(out, "    %04zX: ", i);

        for (size_t j = 0; j < bytes_per_line; j++)
        {
            if (j < n)
                fprintf(out, "%02X ", p[i + j]);
            else
                fprintf(out, "   ");
        }

        fprintf(out, " ");

        for (size_t j = 0; j < n; j++) {
            unsigned char ch = p[i + j];
            fprintf(out, "%c", (isprint(ch) ? ch : '.'));
        }

        fprintf(out, "\n");
    }
}

#define exit_function(retval, msg, ...) \
    do { \
        ret = retval; \
        if (msg) fprintf((retval == EXIT_SUCCESS ? stdout : stderr), "%s: " msg "\n", APP_NAME, ##__VA_ARGS__); \
        goto END_FUNCTION; \
    } while(0)

static int cmd_summary(const params_t *params)
{
    int rc = 0;
    int ret = EXIT_FAILURE;
    ldb_stats_t stats = {0};
    ldb_journal_t *journal = NULL;
    struct stat stat_dat = {0};
    struct stat stat_idx = {0};
    char meta[LDB_METADATA_LEN] = {0};

    if ((journal = ldb_alloc()) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    if ((rc = ldb_open(journal, params->path, params->name, params->flags)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    stat(((journal_header_t *) journal)->dat_path, &stat_dat);
    stat(((journal_header_t *) journal)->idx_path, &stat_idx);

    printf("Data:     %s (%lld bytes)\n", ((journal_header_t *) journal)->dat_path, (long long)stat_dat.st_size);
    printf("Index:    %s (%lld bytes)\n", ((journal_header_t *) journal)->idx_path, (long long)stat_idx.st_size);
    printf("Format:   %u\n", ((journal_header_t *) journal)->format);

    ldb_get_meta(journal, meta, sizeof(meta));

    printf("Metadata: \n");
    print_hexdump(stdout, (const unsigned char *)meta, sizeof(meta));

    ldb_stats(journal, 0, UINT64_MAX, &stats);

    if (stats.num_entries == 0) {
        printf("First entry: (none)\n");
        printf("Last entry:  (none)\n");
        printf("Number of entries: 0\n");
    } else {
        char ts1[64] = {0};
        char ts2[64] = {0};

        format_timestamp(stats.min_timestamp, ts1, sizeof(ts1));
        format_timestamp(stats.max_timestamp, ts2, sizeof(ts2));

        printf("First entry: seqnum=%" PRIu64 ", timestamp=%s\n", stats.min_seqnum, ts1);
        printf("Last entry:  seqnum=%" PRIu64 ", timestamp=%s\n", stats.max_seqnum, ts2);
        printf("Number of entries: %zu\n", stats.num_entries);
    }

    ret = EXIT_SUCCESS;

END_FUNCTION:
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static int cmd_bulk(const params_t *params)
{
    int rc = 0;
    int ret = EXIT_FAILURE;
    ldb_stats_t stats = {0};
    ldb_journal_t *journal = NULL;
    ldb_entry_t entries[MAX_ENTRIES] = {{0}};
    char *buf = NULL;
    size_t buf_len = 0;
    uint64_t seq = 0UL;
    uint64_t from_seq = 0UL;
    uint64_t to_seq = 0UL;

    if ((journal = ldb_alloc()) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    if ((rc = ldb_open(journal, params->path, params->name, params->flags)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if ((rc = ldb_stats(journal, 0, UINT64_MAX, &stats)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if (stats.num_entries == 0)
        exit_function(EXIT_SUCCESS, "%s", "(no entries)");

    from_seq = (params->have_from ? params->from : stats.min_seqnum);
    to_seq = (params->have_to ? params->to : stats.max_seqnum);

    if (from_seq < stats.min_seqnum)
        from_seq = stats.min_seqnum;
    if (to_seq > stats.max_seqnum)
        to_seq = stats.max_seqnum;

    if (from_seq > to_seq)
        exit_function(EXIT_FAILURE, "invalid range (%" PRIu64 " > %" PRIu64 ")", from_seq, to_seq);

    if (to_seq < stats.min_seqnum || from_seq > stats.max_seqnum)
        exit_function(EXIT_SUCCESS, "%s", "(no entries in range)");

    buf_len = 1024 * 1024;
    if ((buf = (char *) malloc(buf_len)) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    seq = from_seq;

    while (seq <= to_seq)
    {
        size_t want = MIN(MAX_ENTRIES, (size_t)(to_seq - seq + 1));
        size_t num = 0;

        if ((rc = ldb_read(journal, seq, entries, want, buf, buf_len, &num)) != LDB_OK)
        {
            if (rc == LDB_ERR_NOT_FOUND)
                break;
            else
                exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));
        }

        if (num == 0)
            break;

        // buffer exhausted: entries[num] contains next entry but data == NULL
        if (num < want && entries[num].seqnum != 0 && entries[num].data == NULL)
        {
            char *ptr = NULL;
            size_t need = (size_t) entries[num].data_len + 64;

            while (buf_len < need)
                buf_len *= BUF_GROWTH_FACTOR;

            if ((ptr = (char *) realloc(buf, buf_len)) == NULL)
                exit_function(EXIT_FAILURE, "%s", "out of memory");

            buf = ptr;
        }

        for (size_t i = 0; i < num; i++)
            print_journal_entry(stdout, &entries[i]);

        // set next seqnum to read
        seq = entries[num - 1].seqnum + 1;
    }

    ret = EXIT_SUCCESS;

END_FUNCTION:
    free(buf);
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static int cmd_purge(const params_t *params)
{
    int rc = 0;
    int ret = EXIT_FAILURE;
    ldb_stats_t stats = {0};
    ldb_journal_t *journal = NULL;
    uint64_t seq = 0UL;

    assert(params->have_num != params->have_seq);

    if ((journal = ldb_alloc()) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    if ((rc = ldb_open(journal, params->path, params->name, params->flags)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if ((rc = ldb_stats(journal, 0, UINT64_MAX, &stats)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if (stats.num_entries == 0)
        exit_function(EXIT_SUCCESS, "%s", "(no entries)");

    seq = (params->have_num ? stats.min_seqnum + params->num : params->seq);

    if ((rc = ldb_purge(journal, seq)) < 0)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror((int)rc));

    printf("Removed entries: %d\n", rc);

    ret = EXIT_SUCCESS;

END_FUNCTION:
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static int cmd_rollback(const params_t *params)
{
    int rc = 0;
    int ret = EXIT_FAILURE;
    ldb_stats_t stats = {0};
    ldb_journal_t *journal = NULL;
    uint64_t seq = 0UL;

    assert(params->have_num != params->have_seq);

    if ((journal = ldb_alloc()) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    if ((rc = ldb_open(journal, params->path, params->name, params->flags)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if ((rc = ldb_stats(journal, 0, UINT64_MAX, &stats)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if (stats.num_entries == 0)
        exit_function(EXIT_SUCCESS, "%s", "(no entries)");

    seq = (params->have_num ? (stats.max_seqnum >= params->num ? stats.max_seqnum - params->num : 0) : params->seq);

    if ((rc = ldb_rollback(journal, seq)) < 0)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror((int)rc));

    printf("Removed entries: %d\n", rc);

    ret = EXIT_SUCCESS;

END_FUNCTION:
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static int cmd_repair(const params_t *params)
{
    int rc = 0;
    int ret = EXIT_FAILURE;
    ldb_journal_t *journal = NULL;

    if ((journal = ldb_alloc()) == NULL)
        exit_function(EXIT_FAILURE, "%s", "out of memory");

    if ((rc = ldb_open(journal, params->path, params->name, params->flags)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    printf("Journal OK\n");

    ret = EXIT_SUCCESS;

END_FUNCTION:
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static void parse_args(int argc, char **argv, params_t *params)
{
    int opt = 0;

    static struct option long_opts[] = {
        {"help",     no_argument,       0, 'h'},
        {"path",     required_argument, 0, 'p'},
        {"check",    no_argument,       0, 'c'},
        {"summary",  no_argument,       0, 1000},
        {"bulk",     no_argument,       0, 1001},
        {"repair",   no_argument,       0, 1002},
        {"purge",    no_argument,       0, 1003},
        {"rollback", no_argument,       0, 1004},
        {"from",     required_argument, 0, 'f'},
        {"to",       required_argument, 0, 't'},
        {"num",      required_argument, 0, 'n'},
        {"seq",      required_argument, 0, 's'},
        {0, 0, 0, 0}
    };

    memset(params, 0x00, sizeof(*params));
    params->mode = MODE_SUMMARY;
    params->path = DEFAULT_PATH;

    while ((opt = getopt_long(argc, argv, "hcp:f:t:n:s:", long_opts, NULL)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                exit(EXIT_SUCCESS);
            case 'p':
                params->path = optarg;
                break;
            case 'c':
                params->flags |= LDB_OPEN_CHECK;
                break;
            case 'f':
                if (!parse_u64(optarg, &params->from)) {
                    fprintf(stderr, "%s: invalid --from\n", APP_NAME);
                    exit(EXIT_FAILURE);
                }
                params->have_from = true;
                break;
            case 't':
                if (!parse_u64(optarg, &params->to)) {
                    fprintf(stderr, "%s: invalid --to\n", APP_NAME);
                    exit(EXIT_FAILURE);
                }
                params->have_to = true;
                break;
            case 'n':
                if (!parse_u64(optarg, &params->num) || params->num == 0) {
                    fprintf(stderr, "%s: invalid --num\n", APP_NAME);
                    exit(EXIT_FAILURE);
                }
                params->have_num = true;
                break;
            case 's':
                if (!parse_u64(optarg, &params->seq) || params->seq == 0) {
                    fprintf(stderr, "%s: invalid --seq\n", APP_NAME);
                    exit(EXIT_FAILURE);
                }
                params->have_seq = true;
                break;
            case 1000:
                params->mode = MODE_SUMMARY;
                break;
            case 1001:
                params->mode = MODE_BULK;
                break;
            case 1002:
                params->mode = MODE_REPAIR;
                break;
            case 1003:
                params->mode = MODE_PURGE;
                break;
            case 1004:
                params->mode = MODE_ROLLBACK;
                break;
            default:
                exit(EXIT_FAILURE);
        }
    }

    switch (params->mode)
    {
        case MODE_SUMMARY:
        case MODE_BULK:
            params->flags |= LDB_OPEN_READONLY;
            break;
        case MODE_REPAIR:
            params->flags |= LDB_OPEN_CHECK | LDB_OPEN_REPAIR;
            break;
        case MODE_PURGE:
        case MODE_ROLLBACK:
            if (params->have_num == params->have_seq) {
                fprintf(stderr, "%s: specify exactly one of -n/--num or -s/--seq\n", APP_NAME);
                exit(EXIT_FAILURE);
            }
            break;
        default:
            break;
    }

    if (optind >= argc) {
        fprintf(stderr, "%s: NAME is required\n", APP_NAME);
        exit(EXIT_FAILURE);
    }

    params->name = argv[optind];

    struct stat statbuf = {0};
    if (stat(params->path, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
        fprintf(stderr, "%s: PATH does not exist\n", APP_NAME);
        exit(EXIT_FAILURE);
    }

    char filename[PATH_MAX] = {0};
    snprintf(filename, sizeof(filename), "%s%s%s%s", 
             params->path,
             (params->path[strlen(params->path) - 1] == PATH_SEPARATOR[0] ? "" : PATH_SEPARATOR),
             params->name,
             EXT_DAT);

    if (access(filename, F_OK) != 0) {
        fprintf(stderr, "%s: %s does not exist\n", APP_NAME, filename);
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char **argv)
{
    params_t params;

    parse_args(argc, argv, &params);

    tzset(); // Honor TZ environment variable

    switch (params.mode)
    {
        case MODE_SUMMARY:
            return cmd_summary(&params);
        case MODE_BULK:
            return cmd_bulk(&params);
        case MODE_REPAIR:
            return cmd_repair(&params);
        case MODE_PURGE:
            return cmd_purge(&params);
        case MODE_ROLLBACK:
            return cmd_rollback(&params);
        default:
            break;
    }

    return EXIT_FAILURE;
}

#ifdef USE_DEFAULTS
void print_journal_entry(FILE *out, const ldb_entry_t *entry)
{
    char ts[64] = {0};

    format_timestamp(entry->timestamp, ts, sizeof(ts));

    fprintf(out, "seqnum=%" PRIu64 ", timestamp=%s, data_len=%u\n", entry->seqnum, ts, entry->data_len);
    print_hexdump(out, (const unsigned char *)entry->data, entry->data_len);
}
#endif
