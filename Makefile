# -Wconversion not set because acutest.h and tests.c warnings pollutes output
CFLAGS= -std=c99 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -Wnull-dereference -pthread
LDFLAGS= -lpthread

all: tests coverage cppcheck valgrind example

tests: logdb.h tests.c
	$(CC) -g $(CFLAGS) -o tests tests.c $(LDFLAGS)
	./tests

example: logdb.h example.c
	$(CC) -g $(CFLAGS) -o example example.c $(LDFLAGS)
	./example

coverage: logdb.h tests.c
	$(CC) --coverage -O0 $(CFLAGS) -o tests-coverage tests.c -lgcov $(LDFLAGS)
	./tests-coverage
	[ -d coverage ] || mkdir coverage
	lcov --no-external -d . -o coverage/coverage.info -c
	genhtml -o coverage coverage/coverage.info

valgrind: logdb.h tests.c
	$(CC) -g $(CFLAGS) -DRUNNING_ON_VALGRIND -o tests-valgrind tests.c $(LDFLAGS)
	valgrind --tool=memcheck --leak-check=yes ./tests-valgrind

helgrind: tests-valgrind
	valgrind --tool=helgrind --history-backtrace-size=50 ./tests-valgrind

cppcheck: logdb.h
	cppcheck --enable=all  --suppress=missingIncludeSystem --suppress=unusedFunction --suppress=checkersReport logdb.h

clean: 
	rm -f tests test.dat test.idx test.tmp
	rm -f example example.dat example.idx example.tmp
	rm -f tests-coverage
	rm -f tests-valgrind
	rm -f *.gcda *.gcno
	rm -rf coverage/
