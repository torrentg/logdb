# -Wconversion not set because acutest.h and tests.c warnings pollutes output
CFLAGS= -std=c99 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -Wnull-dereference -pthread
LDFLAGS= -lpthread

all: tests example1 example2 performance

tests: logdb.h tests.c
	$(CC) -g $(CFLAGS) -DRUNNING_ON_VALGRIND -o tests tests.c $(LDFLAGS)

example1: logdb.h example.c
	$(CC) -g $(CFLAGS) -DLDB_IMPL -o example1 example.c $(LDFLAGS)

example2: logdb.h example.c logdb.c
	$(CC) -g $(CFLAGS) -o example2 example.c logdb.c $(LDFLAGS)

performance: logdb.h performance.c
	$(CC) -g $(CFLAGS) -o performance performance.c $(LDFLAGS)

coverage: logdb.h tests.c
	$(CC) --coverage -O0 $(CFLAGS) -o tests-coverage tests.c -lgcov $(LDFLAGS)
	./tests-coverage
	[ -d coverage ] || mkdir coverage
	lcov --no-external -d . -o coverage/coverage.info -c
	genhtml -o coverage coverage/coverage.info

valgrind: tests
	valgrind --tool=memcheck --leak-check=yes ./tests

helgrind: performance
	valgrind --tool=helgrind --history-backtrace-size=50 ./performance --msw=1 --bpr=10KB --rpc=40 --msr=1 --rpq=100

cppcheck: logdb.h
	cppcheck -DLDB_IMPL --enable=all  --suppress=missingIncludeSystem --suppress=unusedFunction --suppress=checkersReport logdb.h

loc:
	cloc logdb.h tests.c example.c performance.c

clean: 
	rm -f tests test.dat test.idx test.tmp
	rm -f example1 example2 example.dat example.idx example.tmp
	rm -f performance performance.dat performance.idx
	rm -f tests-coverage
	rm -f *.gcda *.gcno
	rm -rf coverage/
