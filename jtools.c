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
#include <libgen.h>
#include "journal.h"

#define APP_NAME                "jtools"
#define EXT_DAT                 ".dat"
#define BATCH_ENTRIES           1024
#define DEFAULT_BUF_SIZE        (1024 * 1024)
#define BUF_GROWTH_FACTOR       2

#define MIN(a,b) (((a) < (b)) ? (a) : (b))

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
    MODE_CHECK,
    MODE_REPAIR,
    MODE_ROLLBACK,
    MODE_PURGE
} mode_e;

typedef struct params_t {
    mode_e mode;
    char path[PATH_MAX];
    char name[64];
    int flags;

    // purge/rollback
    bool have_num;
    bool have_seq;
    uint64_t num;
    uint64_t seq;
} params_t;

static void print_help(FILE *out)
{
    fprintf(out,
        APP_NAME" - journal maintenance tool\n"
        "\n"
        "Usage:\n"
        "  " APP_NAME " -h\n"
        "  " APP_NAME " FILE\n"
        "  " APP_NAME " --check    FILE\n"
        "  " APP_NAME " --repair   FILE\n"
        "  " APP_NAME " --rollback (-n NUM | -s SEQ) FILE\n"
        "  " APP_NAME " --purge    (-n NUM | -s SEQ) FILE\n"
        "\n"
        "Options:\n"
        "  -h, --help              Show this help and exit\n"
        "      --check             Validate journal consistency\n"
        "      --repair            Check and repair journal consistency\n"
        "      --rollback          Remove newest entries (from end)\n"
        "      --purge             Remove oldest entries (from start)\n"
        "  -n, --num=NUM           Number of entries to remove\n"
        "  -s, --seq=SEQ           New boundary (purge keeps from SEQ; rollback keeps up to SEQ)\n"
        "\n"
        "Environment:\n"
        "  TZ                      Time zone used for displaying timestamps\n"
        "\n"
        "Exit codes:\n"
        "  0  Success\n"
        "  1  Failure (invalid args, missing files, locked files, I/O errors, etc.)\n"
    );
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
    } while (0)

static void check_report_cb(const char *msg, void *user_data)
{
    (void) user_data;
    printf("%s\n", msg);
}

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

    if ((rc = ldb_stats(journal, 0, UINT64_MAX, &stats)) != LDB_OK)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    if (stats.min_seqnum == 0) {
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
        printf("Number of entries: %" PRIu64 "\n", stats.max_seqnum - stats.min_seqnum + 1);
    }

    ret = EXIT_SUCCESS;

END_FUNCTION:
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

    if (stats.min_seqnum == 0)
        exit_function(EXIT_SUCCESS, "%s", "(no entries)");

    seq = (params->have_num ? stats.min_seqnum + params->num : params->seq);

    if ((rc = (int) ldb_purge(journal, seq)) < 0)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

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

    if (stats.min_seqnum == 0)
        exit_function(EXIT_SUCCESS, "%s", "(no entries)");

    seq = (params->have_num ? (stats.max_seqnum >= params->num ? stats.max_seqnum - params->num : 0) : params->seq);

    if ((rc = (int) ldb_rollback(journal, seq)) < 0)
        exit_function(EXIT_FAILURE, "%s", ldb_strerror(rc));

    printf("Removed entries: %d\n", rc);

    ret = EXIT_SUCCESS;

END_FUNCTION:
    ldb_close(journal);
    ldb_free(journal);
    return ret;
}

static int cmd_check(const params_t *params, bool repair)
{
    int rc = ldb_check(params->path, params->name, repair, check_report_cb, NULL);

    printf("%s\n", (rc == LDB_OK ? "Journal OK" : "Journal has issues"));

    return (rc == LDB_OK ? EXIT_SUCCESS : EXIT_FAILURE);
}

static void parse_args(int argc, char **argv, params_t *params)
{
    int opt = 0;

    static struct option long_opts[] = {
        {"help",     no_argument,       0, 'h'},
        {"check",    no_argument,       0, 1001},
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

    while ((opt = getopt_long(argc, argv, "hn:s:", long_opts, NULL)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                exit(EXIT_SUCCESS);
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
            case 1001:
                params->mode = MODE_CHECK;
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
        case MODE_CHECK:
            params->flags |= LDB_OPEN_READONLY;
            break;
        case MODE_REPAIR:
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
        fprintf(stderr, "%s: FILE is required\n", APP_NAME);
        exit(EXIT_FAILURE);
    }

    const char *filepath = argv[optind];
    size_t flen = strlen(filepath);

    if (flen < 5 || strcmp(filepath + flen - 4, EXT_DAT) != 0) {
        fprintf(stderr, "%s: FILE must end with %s\n", APP_NAME, EXT_DAT);
        exit(EXIT_FAILURE);
    }

    if (access(filepath, F_OK) != 0) {
        fprintf(stderr, "%s: %s does not exist\n", APP_NAME, filepath);
        exit(EXIT_FAILURE);
    }

    strncpy(params->path, filepath, sizeof(params->path) - 1);
    dirname(params->path);

    char tmp[PATH_MAX] = {0};
    strncpy(tmp, filepath, PATH_MAX - 1);
    char *base = basename(tmp);
    base[strlen(base) - 4] = '\0';
    strncpy(params->name, base, sizeof(params->name) - 1);
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
        case MODE_CHECK:
            return cmd_check(&params, false);
        case MODE_REPAIR:
            return cmd_check(&params, true);
        case MODE_ROLLBACK:
            return cmd_rollback(&params);
        case MODE_PURGE:
            return cmd_purge(&params);
        default:
            break;
    }

    return EXIT_FAILURE;
}
