# -Wconversion not set because acutest.h and tests.c warnings pollutes output
CFLAGS= -std=c99 -D_GNU_SOURCE -Wall -Wextra -Wpedantic -Wnull-dereference -pthread
LDFLAGS= -lpthread

TARGETS = tests example performance jtools

.PHONY: all clean coverage profiler valgrind helgrind cppcheck loc complexity

SANITIZE ?=
ifneq ($(SANITIZE),)
	CFLAGS  += -fsanitize=address -fsanitize=undefined -fsanitize=float-cast-overflow -fsanitize-address-use-after-scope -fno-sanitize-recover -fno-omit-frame-pointer
	LDFLAGS += -fsanitize=address -fsanitize=undefined
endif

SANITIZE_THREADS ?=
ifneq ($(SANITIZE_THREADS),)
	CFLAGS  += -fsanitize=thread -fno-omit-frame-pointer
	LDFLAGS += -fsanitize=thread
endif

all: $(TARGETS)

tests: tests.c journal.h  journal.c
	$(CC) -g $(CFLAGS) -DRUNNING_ON_VALGRIND -o $@ tests.c $(LDFLAGS)

example: example.c journal.h journal.c
	$(CC) -g $(CFLAGS) -o $@ example.c journal.c $(LDFLAGS)

performance: performance.c journal.h journal.c
	$(CC) -g $(CFLAGS) -O2 -o $@ performance.c journal.c $(LDFLAGS)

jtools: jtools.c journal.h journal.c
	$(CC) -g $(CFLAGS) -O2 -DUSE_DEFAULTS -o $@ journal.c jtools.c $(LDFLAGS)

coverage: tests.c journal.h journal.c
	$(CC) --coverage -O0 $(CFLAGS) -o tests-coverage tests.c -lgcov $(LDFLAGS)
	./tests-coverage
	[ -d coverage ] || mkdir coverage
	lcov --no-external -d . -o coverage/coverage.info -c
	genhtml -o coverage coverage/coverage.info

profiler: performance.c journal.h journal.c
	$(CC) -g $(CFLAGS) -pg -O2 -o performance-profiler performance.c journal.c $(LDFLAGS)
	./performance-profiler --bpr=10KB --msw=10 --rpc=40 --msr=10 --rpq=40
	gprof ./performance-profiler gmon.out > performance-profiler.txt

valgrind: tests
	valgrind --tool=memcheck --leak-check=yes ./tests

helgrind: performance
	valgrind --tool=helgrind --history-backtrace-size=50 ./performance --msw=1 --bpr=10KB --rpc=40 --msr=1 --rpq=100

cppcheck: journal.h journal.c
	cppcheck --enable=all --suppress=missingIncludeSystem --suppress=unusedFunction --suppress=assertWithSideEffect --suppress=checkersReport journal.c

loc:
	cloc journal.h journal.c tests.c example.c performance.c jtools.c

complexity:
	lizard -C 20 journal.c

clean:
	rm -f $(TARGETS)
	rm -f *.dat *.idx *.tmp *.gcda *.gcno
	rm -f tests-coverage
	rm -f performance-profiler*
	rm -f gmon.out
	rm -rf coverage/
