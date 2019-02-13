#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

void exec(void *m);

pthread_mutex_t mut1;
pthread_cond_t cond1;

char *shared_msg="todo";


int main5(int argc, char **argv) {
    pthread_t p1,p2, p3;
    pthread_mutex_init(&mut1, NULL);
    pthread_cond_init(&cond1, NULL);
    pthread_create(&p1, NULL, exec, "t1");
    pthread_create(&p2, NULL, exec, "t2");
    pthread_create(&p3, NULL, exec, "t3");
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);
    pthread_mutex_destroy(&mut1);
    printf("finished!%s\n",shared_msg);
    return 0;
}

void exec(void *m) {
    printf("entro: %s\n",m);
    sleep(3);
    pthread_mutex_lock(&mut1);
    if (strcmp (shared_msg,"todo") == 0) {
        //sono il primo, rilascio lock e comincio a lavorare
        printf("doing: %s\n",m);
        shared_msg = "doing";
        pthread_mutex_unlock(&mut1);
        sleep(10);
        pthread_mutex_lock(&mut1);
        printf("done: %s\n",m);
        shared_msg="done";
        pthread_cond_broadcast(&cond1);
    } else if (strcmp (shared_msg,"doing") == 0) {
        //non sono il primo, attendo segnale di sblocco
        printf("wait: %s\n",m);
        pthread_cond_wait(&cond1, &mut1);
        printf("awake: %s\n",m);
    }
    pthread_mutex_unlock(&mut1);
    printf("esco: %s\n",(char*)m);
}

/*
 entro: t1
 entro: t3
 entro: t2
 doing: t1
 wait: t3
 wait: t2
 done: t1
 esco: t1
 awake: t3
 esco: t3
 awake: t2
 esco: t2
 finished!done
 Program ended with exit code: 0
 */

