#!/bin/sh
gcc prod_cons_test.c prod_cons_pthreads.c -lcunit -lpthread -o prod_cons_test
#gcc -c prod_cons_pthreads.c -lpthread
#gcc -c prod_cons_test.c prod_cons_pthreads.o -lcunit -lpthread -o prod_cons_test

./prod_cons_test
