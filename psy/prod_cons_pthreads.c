#include "prod_cons_pthreads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

//viene creata una copia "privata" della stringa
msg_t* msg_init(void* content) {
    msg_t* new_msg = (msg_t*)malloc( sizeof(msg_t) );
    char* string = (char*)content;
    char* new_content = (char*)malloc(strlen(string)+1); // +1 per \0 finale
    strcpy(new_content, string);
    new_msg->content = new_content;
    new_msg->msg_init = msg_init;
    new_msg->msg_destroy = msg_destroy;
    new_msg->msg_copy = msg_copy;
    return new_msg;
}

//viene deallocato il messaggio e il suo contenuto
void msg_destroy(msg_t* msg) {
    free(msg->content); // free copia privata
    free(msg); // free struct
}

//viene copiato il messaggio
msg_t* msg_copy(msg_t* msg) {
    return msg->msg_init(msg->content);
}

/* allocazione / deallocazione buffer */
// creazione di un buffer vuoto di dim. max nota
buffer_t* buffer_init(unsigned int maxsize){
    buffer_t* buffer = (buffer_t*)malloc(sizeof(buffer_t));
    buffer->elems = (msg_t*)malloc(sizeof(msg_t)*maxsize);
    //dimensione e indici
    buffer->max_size = maxsize;
    buffer->size = 0;
    buffer->put_i = 0;
    buffer->get_i = 0;
    //strumenti sincronizzazione
    pthread_mutex_init(&buffer->buff_mut, NULL);
    pthread_cond_init(&buffer->not_full_cond, NULL);
    pthread_cond_init(&buffer->not_empty_cond, NULL);
    
    return buffer;
}

// deallocazione di un buffer
void buffer_destroy(buffer_t* buffer){
    //distruzione strumenti sincronizzazione
    pthread_cond_destroy(&buffer->not_empty_cond);
    pthread_cond_destroy(&buffer->not_full_cond);
    pthread_mutex_destroy(&buffer->buff_mut);
    //deallocazione buffer e contenuto
    free(buffer->elems);
    free(buffer);
}

/* operazioni sul buffer */
// inserimento bloccante: sospende se pieno, quindi
// effettua l'inserimento non appena si libera dello spazio
// restituisce il messaggio inserito; N.B.: msg!=null
msg_t* put_bloccante(buffer_t* buffer, msg_t* msg){
    //acquisisco lock buffer
    pthread_mutex_lock(&buffer->buff_mut);
    //verifco se buffer è pieno
    while ((buffer->size) == buffer->max_size) {
        //buffer pieno, attendo prossima estrazione rilasciando lock buffer
//        printf("richiesto inserimento bloccante con buffer pieno: attendo prossima estrazione...\n");
        pthread_cond_wait(&buffer->not_full_cond, &buffer->buff_mut);
    }
    //buffer non pieno, inserisco elemento
//    printf("richiesto inserimento bloccante con buffer non pieno: procedo all'inserimento...\n");
    buffer->elems[buffer->put_i % buffer->max_size] = *msg;
    //incremento indice inserimenti
    buffer->put_i++;
    buffer->size++;
    //notifico eventuale consumatore in attesa se precedentemente buffer era vuoto
    pthread_cond_signal(&buffer->not_empty_cond);
    //rilascio lock buffer
    pthread_mutex_unlock(&buffer->buff_mut);
    return msg;
}

// inserimento non bloccante: restituisce BUFFER_ERROR se pieno,
// altrimenti effettua l'inserimento e restituisce il messaggio
// inserito; N.B.: msg!=null
msg_t* put_non_bloccante(buffer_t* buffer, msg_t* msg){
    //acquisisco lock buffer
    pthread_mutex_lock(&buffer->buff_mut);
    //verifco se buffer è pieno
    if (buffer->size == buffer->max_size) {
        //buffer pieno, restituisco BUFFER_ERROR
//        printf("richiesto inserimento non bloccante con buffer pieno: restituisco errore...\n");
        pthread_mutex_unlock(&buffer->buff_mut);
        return BUFFER_ERROR;
    }
    //buffer non pieno, inserisco elemento
//    printf("richiesto inserimento non bloccante con buffer non pieno: procedo all'inserimento...\n");
    buffer->elems[buffer->put_i % buffer->max_size] = *msg;
    //incremento indice inserimenti
    buffer->put_i++;
    buffer->size++;
    //notifico eventuale consumatore in attesa
    pthread_cond_signal(&buffer->not_empty_cond);
    //rilascio lock buffer
    pthread_mutex_unlock(&buffer->buff_mut);
    return msg;
}

// estrazione bloccante: sospende se vuoto, quindi
// restituisce il valore estratto non appena disponibile
msg_t* get_bloccante(buffer_t* buffer){
    //acquisisco lock buffer
    pthread_mutex_lock(&buffer->buff_mut);
    //verifco se buffer è vuoto
    while (buffer->size == 0) {
        //buffer vuoto, attendo prossimo inserimento
//        printf("richiesta estrazione bloccante con buffer vuoto: attendo prossimo inserimento...\n");
        pthread_cond_wait(&buffer->not_empty_cond, &buffer->buff_mut);
    }
//    printf("richiesta estrazione bloccante con buffer non vuoto: procedo all'estrazione...\n");
    //buffer non vuoto, estraggo elemento
    msg_t* ret = msg_copy(&(buffer->elems[buffer->get_i % buffer->max_size]));
    //imposto a NULL elemento dell'array consumato
    buffer->elems[buffer->get_i % buffer->max_size].content = NULL;
    //incremento indice estrazioni
    buffer->get_i++;
    buffer->size--;
    //notifico eventuale produttore in attesa se precedentemente buffer era pieno
    pthread_cond_signal(&buffer->not_full_cond);
    //rilascio lock buffer
    pthread_mutex_unlock(&buffer->buff_mut);
    return ret;
}

// estrazione non bloccante: restituisce BUFFER_ERROR se vuoto
// ed il valore estratto in caso contrario
msg_t* get_non_bloccante(buffer_t* buffer){
    //acquisisco lock buffer
    pthread_mutex_lock(&buffer->buff_mut);
    //verifco se buffer è vuoto
    if (buffer->size == 0) {
        //buffer vuoto, restituisco BUFFER_ERROR
//        printf("richiesta estrazione non bloccante con buffer vuoto: restituisco errore...\n");
        pthread_mutex_unlock(&buffer->buff_mut);
        return BUFFER_ERROR;
    }
//    printf("richiesta estrazione bloccante con buffer non vuoto: procedo all'estrazione...\n");
    //buffer non vuoto, estraggo elemento
    msg_t* ret = msg_copy(&(buffer->elems[buffer->get_i % buffer->max_size]));
    //imposto a NULL elemento dell'array consumato
    buffer->elems[buffer->get_i % buffer->max_size].content = NULL;
    //incremento indice estrazioni
    buffer->get_i++;
    buffer->size--;
    //notifico eventuale produttore in attesa se precedentemente buffer era pieno
    pthread_cond_signal(&buffer->not_full_cond);
    //rilascio lock buffer
    pthread_mutex_unlock(&buffer->buff_mut);
    return ret;
}
