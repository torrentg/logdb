#include <signal.h>
#include <getopt.h>

#define LDB_IMPL
#include "logdb.h"

typedef struct {
    bool truncate_db;
    bool force_sync;
} params_db_t;

typedef struct {
    size_t bytes_per_record;
    size_t records_per_commit;
    size_t records_per_second;
    size_t max_seconds;
    size_t max_records;
    size_t max_bytes;
} params_write_t;

typedef struct {
    size_t time_ms;
    size_t idle_ms;
    size_t num_records;
    size_t num_bytes;
    size_t num_commits;
    int rc;
} results_write_t;

typedef struct {
    ldb_db_t *db;
    params_write_t params;
    results_write_t results;
} args_write_t;

typedef struct {
    size_t records_per_second;
    size_t records_per_query;
    size_t max_seconds;
    size_t max_records;
    size_t max_bytes;
} params_read_t;

typedef struct {
    size_t time_ms;
    size_t idle_ms;
    size_t num_records;
    size_t num_bytes;
    size_t num_queries;
    int rc;
} results_read_t;

typedef struct {
    ldb_db_t *db;
    params_read_t params;
    results_read_t results;
} args_read_t;


static volatile bool interrupted = false;
static const char *bytes_suffix[] = {"B", "KB", "MB", "GB", "TB"};
#define BYTES_SUFFIX_LEN (sizeof(bytes_suffix)/sizeof(bytes_suffix[0]))

static void signal_handler(int signum)
{
    (void)(signum);
    interrupted = true;
}

static const char * bytes2str(size_t bytes, uint8_t num_decimals)
{
    size_t i;
    double dblBytes = bytes;

    for (i = 0; (bytes / 1000) > 0 && i < BYTES_SUFFIX_LEN-1; i++, bytes /= 1000)
        dblBytes = bytes / 1000.0;

    static char output[200];
    sprintf(output, "%.*lf %s", num_decimals, dblBytes, bytes_suffix[i]);
    return output;
}

static int msleep(long msec)
{
    int res;
    struct timespec ts;

    if (msec < 0) {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}

static void print_results_write(results_write_t *results)
{
    double seconds = (double) results->time_ms / 1000.0;
    printf("write - result         = %s\n", ldb_strerror(results->rc));
    printf("write - total time     = %.2lf seconds\n", seconds);
    printf("write - idle time      = %.2lf seconds\n", (double) results->idle_ms / 1000.0);
    printf("write - total records  = %zu\n", results->num_records);
    printf("write - total size     = %s\n", bytes2str(results->num_bytes, 2));
    printf("write - total commits  = %zu\n", results->num_commits);
    printf("write - records/second = %.2lf\n", (double) results->num_records / seconds);
    printf("write - bytes/second   = %s\n", bytes2str(results->num_bytes / seconds, 2));
    printf("write - commits/second = %.2lf\n", (double) results->num_commits / seconds);
    printf("write - idle time (%%)  = %d%%\n", (int)(100.0 * (double) results->idle_ms / (double) results->time_ms));
}

static void print_results_read(results_read_t *results)
{
    double seconds = results->time_ms / 1000.0;
    printf("read  - result         = %s\n", ldb_strerror(results->rc));
    printf("read  - total time     = %.2lf seconds\n", seconds);
    printf("read  - idle time      = %.2lf seconds\n", (double) results->idle_ms / 1000.0);
    printf("read  - total records  = %zu\n", results->num_records);
    printf("read  - total size     = %s\n", bytes2str(results->num_bytes, 2));
    printf("read  - total queries  = %zu\n", results->num_queries);
    printf("read  - records/second = %.2lf\n", (double) results->num_records / seconds);
    printf("read  - bytes/second   = %s\n", bytes2str(results->num_bytes / seconds, 2));
    printf("read  - queries/second = %.2lf\n", (double) results->num_queries / seconds);
    printf("read  - idle time (%%)  = %d%%\n", (int)(100.0 * (double) results->idle_ms / (double) results->time_ms));
}

static void * run_write(void *args)
{
    ldb_db_t *db = ((args_write_t *) args)->db;
    params_write_t *params = &((args_write_t *) args)->params;
    results_write_t *results = &((args_write_t *) args)->results;

    char *data = calloc(params->bytes_per_record, 1);
    size_t num_entries = ldb_min(params->records_per_commit, params->records_per_second);
    ldb_entry_t *entries = calloc(num_entries, sizeof(ldb_entry_t));
    uint64_t time0 = ldb_get_millis();
    size_t num = 0;

    // all records have always the same content
    for (size_t i = 0; i < num_entries; i++) {
        entries[i].metadata_len = 0;
        entries[i].metadata = NULL;
        entries[i].data_len = params->bytes_per_record;
        entries[i].data = data;
    }

    *results = (results_write_t){0};
    results->rc = LDB_OK;

    while ( !interrupted &&
            results->rc == LDB_OK &&
            results->time_ms < 1000*params->max_seconds && 
            results->num_records < params->max_records &&
            results->num_bytes < params->max_bytes)
    {
        for (size_t i = 0; i < num_entries; i++) {
            entries[i].seqnum = 0;
            entries[i].timestamp = 0;
        }

        if ((results->rc = ldb_append(db, entries, num_entries, &num)) != LDB_OK)
            break;

        results->num_commits += (num > 0 ? 1 : 0);
        results->num_records += num;
        results->num_bytes += num * params->bytes_per_record;

        while(true)
        {
            results->time_ms = ldb_get_millis() - time0;
            if (results->time_ms >= 1000*params->max_seconds)
                break;

            double seconds = (double) results->time_ms / 1000.0;
            if ((double) results->num_records < seconds * params->records_per_second)
                break;

            results->idle_ms++;
            msleep(1);
        };
    }

    free(entries);
    free(data);
    return NULL;
}

static void * run_read(void *args)
{
    ldb_db_t *db = ((args_read_t *) args)->db;
    params_read_t *params = &((args_read_t *) args)->params;
    results_read_t *results = &((args_read_t *) args)->results;

    size_t num_entries = params->records_per_query;
    ldb_entry_t *entries = calloc(num_entries, sizeof(ldb_entry_t));
    uint64_t time0 = ldb_get_millis();
    ldb_stats_t stats = {0};
    uint64_t seqnum = 0;
    size_t num = 0;

    *results = (results_read_t){0};
    results->rc = LDB_OK;

    while ( !interrupted &&
            results->rc == LDB_OK &&
            results->time_ms < 1000*params->max_seconds && 
            results->num_records < params->max_records &&
            results->num_bytes < params->max_bytes)
    {
        if ((results->rc = ldb_stats(db, 0, SIZE_MAX, &stats)) != LDB_OK)
            break;

        if (stats.num_entries)
        {
            seqnum = stats.min_seqnum + rand() % stats.num_entries;

            if ((results->rc = ldb_read(db, seqnum, entries, num_entries, &num)) != LDB_OK)
                break;

            results->num_queries += (num > 0 ? 1 : 0);
            results->num_records += num;

            for (size_t i = 0; i < num ; i++)
                results->num_bytes += entries[i].metadata_len + entries[i].data_len;
        }

        while (true)
        {
            results->time_ms = ldb_get_millis() - time0;
            if (results->time_ms >= 1000*params->max_seconds)
                break;

            double seconds = (double) results->time_ms / 1000.0;
            if ((double) results->num_records <= seconds * params->records_per_second)
                break;

            results->idle_ms++;
            msleep(1);
        }
    }

    ldb_free_entries(entries, num_entries);
    free(entries);
    return NULL;
}

static void help(void)
{
    const char *msg = \
        "usage: performance [OPTION]..." "\n" \
        "\n" \
        "Tool used to test your logdb workload." "\n" \
        "\n" \
        "Arguments:" "\n" \
        "   -h, --help                          Display this help and quit." "\n" \
        "   -s, --force-sync                    Force sync after flush." "\n" \
        "   -a, --append                        Preserve existing db (truncated by default)" "\n" \
        "   --bpr, --bytes-per-record           Bytes per record (allowed suffixes: B, KB, MB, GB, TB)." "\n" \
        "   --rpc, --records-per-commit         Records per commit." "\n" \
        "   --rpq, --records-per-query          Records per query." "\n" \
        "   --msw, --max-seconds-write          Maximum number of seconds." "\n" \
        "   --msr, --max-seconds-read           Maximum number of seconds." "\n" \
        "   --mrw, --max-records-write          Maximum number of records." "\n" \
        "   --mrr, --max-records-read           Maximum number of records." "\n" \
        "   --mbw, --max-bytes-write            Maximum number of bytes (allowed suffixes: B, KB, MB, GB, TB)." "\n" \
        "   --mbr, --max-bytes-read             Maximum number of bytes (allowed suffixes: B, KB, MB, GB, TB)." "\n" \
        "   --rpsw, --records-per-second-write  Records per second writing." "\n" \
        "   --rpsr, --records-per-second-read   Records per second reading." "\n" \
        "\n" \
        "Examples:" "\n" \
        "   # record size = 10KB" "\n" \
        "   # writing at full speed for 10 seconds" "\n" \
        "   # reading at full-speed for 10 seconds" "\n" \
        "   performance --bpr=10KB --msw=10 --rpc=40 --msr=10 --rpq=40" "\n" \
        "\n" \
        "   # record size = 10KB" "\n" \
        "   # writing 1GB at full speed" "\n" \
        "   # reading 250000 records at full speed" "\n" \
        "   performance --bpr=10KB --mbw=1GB --rpc=40 --mrr=250000 --rpq=100" "\n" \
        "\n" \
        "   # record size = 10KB" "\n" \
        "   # writing 10000 records/sec for 10 seconds" "\n" \
        "   # reading 6000 records/sec for 10 seconds" "\n" \
        "   performance --msw=10 --bpr=10KB --rpsw=10000 --rpc=40 --msr=10 --rpsr=6000 --rpq=100" "\n" \
        "\n";

    printf("%s", msg);
}

static size_t parse_int(const char *str, const char *arg)
{
    char *endptr = NULL;
    long val = strtol(str, &endptr, 10);

    if (!isdigit(*str) || errno == ERANGE || str == endptr || *endptr != 0) {
        fprintf(stderr, "Error: argument '%s' has an invalid value (%s)\n", arg, str);
        exit(EXIT_FAILURE);
    }

    return val;
}

static size_t parse_bytes(const char *str, const char *arg)
{
    char *endptr = NULL;
    long val = strtol(str, &endptr, 10);

    if (!isdigit(*str) || errno == ERANGE || str == endptr) {
        fprintf(stderr, "Error: argument '%s' has an invalid value (%s)\n", arg, str);
        exit(EXIT_FAILURE);
    }

    if (*endptr == 0)
        return val;

    for (size_t i = 0; i < BYTES_SUFFIX_LEN-1; i++) {
        if (strcmp(endptr, bytes_suffix[i]) == 0)
            return val;
        val *= 1000;
    }

    fprintf(stderr, "Error: argument '%s' has an invalid value (%s)\n", arg, str);
    exit(EXIT_FAILURE);
}

static void parse_args(int argc, char *argv[], params_db_t *params_db, params_write_t *params_write, params_read_t *params_read)
{
    const char* const options1 = "has" ;
    const struct option options2[] = {
        { "help",                     0,  NULL,  'h' },
        { "append",                   0,  NULL,  'a' },
        { "force-sync",               0,  NULL,  's' },
        { "bytes-per-record",         1,  NULL,  301 },
        { "bpr",                      1,  NULL,  301 },
        { "records-per-commit",       1,  NULL,  302 },
        { "rpc",                      1,  NULL,  302 },
        { "records-per-query",        1,  NULL,  303 },
        { "rpq",                      1,  NULL,  303 },
        { "max-seconds-write",        1,  NULL,  304 },
        { "msw",                      1,  NULL,  304 },
        { "max-seconds-read",         1,  NULL,  305 },
        { "msr",                      1,  NULL,  305 },
        { "max-records-write",        1,  NULL,  306 },
        { "mrw",                      1,  NULL,  306 },
        { "max-records-read",         1,  NULL,  307 },
        { "mrr",                      1,  NULL,  307 },
        { "max-bytes-write",          1,  NULL,  308 },
        { "mbw",                      1,  NULL,  308 },
        { "max-bytes-read",           1,  NULL,  309 },
        { "mbr",                      1,  NULL,  309 },
        { "records-per-second-write", 1,  NULL,  310 },
        { "rpsw",                     1,  NULL,  310 },
        { "records-per-second-read",  1,  NULL,  311 },
        { "rpsr",                     1,  NULL,  311 },
        { NULL,                       0,  NULL,   0  }
    };

    *params_db = (params_db_t) {
        .truncate_db = true,
        .force_sync = false
    };

    *params_write = (params_write_t){
        .bytes_per_record = 0,
        .records_per_commit = 0,
        .records_per_second = SIZE_MAX,
        .max_seconds = SIZE_MAX,
        .max_records = SIZE_MAX,
        .max_bytes = SIZE_MAX
    };

    *params_read = (params_read_t){
        .records_per_query = 0,
        .records_per_second = SIZE_MAX,
        .max_seconds = SIZE_MAX,
        .max_records = SIZE_MAX,
        .max_bytes = SIZE_MAX
    };

    while (true)
    {
        int curropt = getopt_long(argc, argv, options1, options2, NULL);

        if (curropt == -1)
            break;

        switch(curropt)
        {
            case '?': // invalid option
                fprintf(stderr, "use --help option for more information\n");
                exit(EXIT_FAILURE);
            case 'h':
                help();
                exit(EXIT_SUCCESS);
            case 'a':
                params_db->truncate_db = false;
                break;
            case 's':
                params_db->force_sync = true;
                break;
            case 301:
                params_write->bytes_per_record = parse_bytes(optarg, "bytes-per-record");
                break;
            case 302:
                params_write->records_per_commit = parse_int(optarg, "records-per-commit");
                break;
            case 303:
                params_read->records_per_query = parse_int(optarg, "records-per-query");
                break;
            case 304:
                params_write->max_seconds = parse_int(optarg, "max-seconds-write");
                break;
            case 305:
                params_read->max_seconds = parse_int(optarg, "max-seconds-read");
                break;
            case 306:
                params_write->max_records = parse_int(optarg, "max-records-write");
                break;
            case 307:
                params_read->max_records = parse_int(optarg, "max-records-read");
                break;
            case 308:
                params_write->max_bytes = parse_bytes(optarg, "max-bytes-write");
                break;
            case 309:
                params_read->max_bytes = parse_bytes(optarg, "max-bytes-read");
                break;
            case 310:
                params_write->records_per_second = parse_int(optarg, "records-per-second-write");
                break;
            case 311:
                params_read->records_per_second = parse_int(optarg, "records-per-second-read");
                break;
            default:
                fprintf(stderr, "Unexpected error\n");
                exit(EXIT_FAILURE);
        }
    }

    if (params_write->max_bytes == SIZE_MAX &&
        params_write->max_records == SIZE_MAX &&
        params_write->max_seconds == SIZE_MAX)
    {
        fprintf(stderr, "Error: Write stop criteria not found\n");
        fprintf(stderr, "Set max-records-write or max-seconds-write or max-bytes-write\n");
        fprintf(stderr, "use --help option for more information\n");
        exit(EXIT_FAILURE);
    }

    if (params_write->bytes_per_record == 0) {
        fprintf(stderr, "Error: bytes-per-record not set\n");
        exit(EXIT_FAILURE);
    }

    if (params_write->records_per_commit == 0) {
        fprintf(stderr, "Error: records-per-commit not set\n");
        exit(EXIT_FAILURE);
    }

    if (params_read->max_bytes == SIZE_MAX &&
        params_read->max_records == SIZE_MAX &&
        params_read->max_seconds == SIZE_MAX)
    {
        fprintf(stderr, "Error: Read stop criteria not found\n");
        fprintf(stderr, "Set max-records-read or max-seconds-read or max-bytes-read\n");
        fprintf(stderr, "use --help option for more information\n");
        exit(EXIT_FAILURE);
    }

    if (params_read->records_per_query == 0) {
        fprintf(stderr, "Error: records-per-query not set\n");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[])
{
    params_db_t params_db = {0};
    params_write_t params_write = {0};
    params_read_t params_read = {0};

    parse_args(argc, argv, &params_db, &params_write, &params_read);

    if (params_db.truncate_db) {
        remove("performance.dat");
        remove("performance.idx");
    }

    srand(time(NULL));
    signal(SIGINT, signal_handler);

    ldb_db_t db = {0};
    if (ldb_open("", "performance", &db, false) != LDB_OK) {
        fprintf(stderr, "error opening database\n");
        return EXIT_FAILURE;
    }

    db.force_fsync = params_db.force_sync;

    pthread_t thread_write;
    args_write_t args_write = { .db = &db, .params = params_write };
    pthread_create(&thread_write, NULL, run_write, &args_write); 

    pthread_t thread_read;
    args_read_t args_read = { .db = &db, .params = params_read };
    pthread_create(&thread_read, NULL, run_read, &args_read); 

    pthread_join(thread_write, NULL);
    pthread_join(thread_read, NULL);

    print_results_write(&args_write.results);
    print_results_read(&args_read.results);

    ldb_close(&db);
    return EXIT_SUCCESS;
}
