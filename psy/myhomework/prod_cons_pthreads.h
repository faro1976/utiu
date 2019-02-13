#include <stdio.h>
#include <pthread.h>

#define BUFFER_ERROR (msg_t *) NULL

//tipo messaggio
typedef struct msg_item {
    void* content; // generico contenuto del messaggio
    struct msg_item * (*msg_init)(void*); // creazione msg
    void (*msg_destroy)(struct msg_item *); // deallocazione msg
    struct msg_item * (*msg_copy)(struct msg_item *); // creazione/copia msg
} msg_t;
msg_t* msg_init(void* content);
void msg_destroy(msg_t* msg);
msg_t* msg_copy(msg_t* msg);

//tipo buffer
typedef struct buffer {
    //elementi buffer
    msg_t* elems;
    //sincronizzazione
    pthread_mutex_t buff_mut;
    pthread_cond_t not_full_cond, not_empty_cond;
    //capienza buffer
    int max_size;
    //dimensione corrente buffer
    int size;
    //indice inserimenti
    int put_i;
    //indice estrazioni
    int get_i;
} buffer_t;

/* allocazione / deallocazione buffer */
// creazione di un buffer vuoto di dim. max nota
buffer_t* buffer_init(unsigned int maxsize);
// deallocazione di un buffer
void buffer_destroy(buffer_t* buffer);

/* operazioni sul buffer */
// inserimento bloccante: sospende se pieno, quindi
// effettua l'inserimento non appena si libera dello spazio
// restituisce il messaggio inserito; N.B.: msg!=null
msg_t* put_bloccante(buffer_t* buffer, msg_t* msg);
// inserimento non bloccante: restituisce BUFFER_ERROR se pieno,
// altrimenti effettua l'inserimento e restituisce il messaggio
// inserito; N.B.: msg!=null
msg_t* put_non_bloccante(buffer_t* buffer, msg_t* msg);
// estrazione bloccante: sospende se vuoto, quindi
// restituisce il valore estratto non appena disponibile
msg_t* get_bloccante(buffer_t* buffer);
// estrazione non bloccante: restituisce BUFFER_ERROR se vuoto
// ed il valore estratto in caso contrario
msg_t* get_non_bloccante(buffer_t* buffer);
