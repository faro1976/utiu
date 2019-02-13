#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include "CUnit/Basic.h"
#include "CUnit/TestDB.h"
#include "prod_cons_pthreads.h"

buffer_t* buffer_pieno;
buffer_t* buffer_vuoto;
msg_t *expected_msg;
char expected_msg_str[] = "expected_msg";
char go_msg_str[] = "go_msg";
char prefill_msg_str[] = "prefill_msg";
int buff_n_size = 5;

//init/clean suites (unitario ed ennario)
int init_suiteBufferUnit(void) {
//    printf("inizio init suiteBufferUnit\n");
    buffer_pieno = buffer_init(1);
    buffer_vuoto = buffer_init(1);
    msg_t* msg = msg_init(prefill_msg_str);
    put_non_bloccante(buffer_pieno, msg);
//    printf("fine init suiteBufferUnit\n");
    return 0;
}

int clean_suiteBufferUnit(void) {
//    printf("inizio clean suiteBufferUnit\n");
    buffer_destroy(buffer_pieno);
    buffer_destroy(buffer_vuoto);
//    printf("fine clean suiteBufferUnit\n");
    return 0;
}

int init_suiteBufferN(void) {
//    printf("inizio init suiteBufferN\n");
    buffer_pieno = buffer_init(buff_n_size);
    buffer_vuoto = buffer_init(buff_n_size);
    int i;
    for (i=0; i<buff_n_size; i++) {
        char msg_str[20];
//        sprintf(msg_str, "%s nr.%d", prefill_msg_str, i);
        msg_t* msg = msg_init(msg_str);
        put_non_bloccante(buffer_pieno, msg);
    }
//    printf("fine init suiteBufferN\n");
    return 0;
}

int clean_suiteBufferN(void) {
//    printf("inizio clean suiteBufferN\n");
    buffer_destroy(buffer_pieno);
    buffer_destroy(buffer_vuoto);
//    printf("fine clean suiteBufferN\n");
    return 0;
}

//funzioni oggetto dei test
//produzione bloccante con esito positivo
msg_t* produzione_bloccante_OK(void* msg_str){
    msg_t* msg = msg_init(msg_str);
    msg_t* ret = put_bloccante(buffer_vuoto, msg);
    CU_ASSERT_STRING_EQUAL(ret->content, msg_str);
    return ret;
}

//produzione non bloccante con esito positivo
msg_t* produzione_non_bloccante_OK(void* msg_str){
    msg_t* msg = msg_init(msg_str);
    msg_t* ret = put_non_bloccante(buffer_vuoto, msg);
    CU_ASSERT_STRING_EQUAL(ret->content, msg_str);
    return ret;
}

//produzione non bloccante con esito negativo
msg_t* produzione_non_bloccante_KO(void* msg_str){
    msg_t* msg = msg_init(msg_str);
    msg_t* ret = put_non_bloccante(buffer_pieno, msg);
    CU_ASSERT_PTR_EQUAL(ret, BUFFER_ERROR);
    //verifica permanenza elementi iniziali
    CU_ASSERT_STRING_EQUAL(buffer_pieno->elems[0].content, prefill_msg_str);
    return ret;
}

//consumazione bloccante con esito positivo
msg_t* consumazione_bloccante_OK(void* msg_str){
    msg_t* ret = get_bloccante(buffer_vuoto);
    CU_ASSERT_STRING_EQUAL(ret->content, msg_str);
    return ret;
}

//consumazione non bloccante con esito positivo
msg_t* consumazione_non_bloccante_OK(void* msg_str){
    msg_t* ret = get_non_bloccante(buffer_vuoto);
    CU_ASSERT_STRING_EQUAL(ret->content, msg_str);
    return ret;
}

//consumazione non bloccante con esito negativo
msg_t* consumazione_non_bloccante_KO(void* msg_str){
    msg_t* ret = get_non_bloccante(buffer_vuoto);
    CU_ASSERT_PTR_EQUAL(ret, BUFFER_ERROR);
    return ret;
}



//test cases

//buffer unitario
//(P=1; C=0; N=1) Produzione non bloccante in un buffer unitario pieno
//il produttore restituisce errore
//funzioni testate: produzione non bloccante KO
void test_produzione_non_bloccante_unit_KO(void){
    clean_suiteBufferUnit();
    init_suiteBufferUnit();
    pthread_t t1;
    pthread_create(&t1, NULL, produzione_non_bloccante_KO, expected_msg_str);
    pthread_join(t1, NULL);
}

//(P=0; C=1; N=1) Consumazione bloccante da un buffer unitario vuoto
//il consumatore attende produzione di un messaggio
//funzioni testate: consumazione bloccante OK, produzione non bloccante OK
void test_consumazione_bloccante_unit_OK(void){
    clean_suiteBufferUnit();
    init_suiteBufferUnit();
    pthread_t c1, p1;
    pthread_create(&c1, NULL, consumazione_bloccante_OK, go_msg_str);
    sleep(2);
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
    pthread_create(&p1, NULL, produzione_non_bloccante_OK, go_msg_str);
    pthread_join(c1, NULL);
    pthread_join(p1, NULL);
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
}

//(P=0; C=1; N=1) Consumazione non bloccante di un solo messaggio da un buffer pieno
//il consumatore consuma regolarmente
//funzioni testate: produzione non bloccante OK, onsumazione non bloccante OK
void test_consumazione_non_bloccante_unit_OK(void){
    clean_suiteBufferUnit();
    init_suiteBufferUnit();
    pthread_t c1, p1;
    pthread_create(&p1, NULL, produzione_non_bloccante_OK, expected_msg_str);
    sleep(2);
    CU_ASSERT_EQUAL(buffer_vuoto->size, 1);
    pthread_create(&c1, NULL, consumazione_non_bloccante_OK, expected_msg_str);
    pthread_join(p1, NULL);
    pthread_join(c1, NULL);
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
}


//buffer ennario
//(P>1; C=1; N>1) Produzione bloccante concorrente di molteplici messaggi in un buffer vuoto; il buffer si satura in corso
//il produttore inserisce fino a saturazione e attende successiva estrazione consumatore per inserimento ultimo messaggio (6 messaggi inseriti per dimensione buffer 5)
//funzioni testate: produzione bloccante OK, produzione bloccante KO (attesa), consumazione non bloccante OK
void test_produzione_bloccante_concorrente_n_OK(){
    clean_suiteBufferN();
    init_suiteBufferN();
    pthread_t pn[buff_n_size+1];
    pthread_t c1;
    pthread_create(&pn[0], NULL, produzione_bloccante_OK, "msg0");
    //attendo per esser certo che msg0 occupi la prima posizione del buffer
    sleep(2);
    pthread_create(&pn[1], NULL, produzione_bloccante_OK, "msg1");
    pthread_create(&pn[2], NULL, produzione_bloccante_OK, "msg2");
    pthread_create(&pn[3], NULL, produzione_bloccante_OK, "msg3");
    pthread_create(&pn[4], NULL, produzione_bloccante_OK, "msg4");
    //attendo per esser certo che msg5 rimanga in attesa di una posizione libera nel buffer
    sleep(2);
    pthread_create(&pn[5], NULL, produzione_bloccante_OK, "msg5");
    CU_ASSERT_STRING_NOT_EQUAL(buffer_vuoto->elems[0].content, "msg5");
    sleep(2);
    pthread_create(&c1, NULL, consumazione_bloccante_OK, "msg0");
    int i;
    for (i=0; i<(buff_n_size+1); i++) {
        pthread_join(pn[i], NULL);
    }
    pthread_join(c1, NULL);
    sleep(2);
    //verifico che il primo elemento del buffer, ultimo ad essere stato inserito, sia ora msg5
    CU_ASSERT_STRING_EQUAL(buffer_vuoto->elems[0].content, "msg5");
    //verifico che il buffer sia nuovamente saturo
    CU_ASSERT_EQUAL(buffer_vuoto->size, buff_n_size);
}

//(P>1; C>1; N>1) Consumazioni e produzioni concorrenti di molteplici messaggi in un buffer vuoto
//nella prima fase ci sono 5 produzioni e 5 consumazioni interlacciate (alla fine il buffer torna ad essere vuoto), nella seconda fase ulteriori 2 consumazioni di cui una bloccante (resta in attesa) e l'altra non bloccante (restituisce errore), nella terza fase due ulteriori produzioni risvegliano la consumazione rimasta in attesa e lasciano un messaggio nel buffer
//funzioni testate: consumazione bloccante OK, consumazione bloccante KO, consumazione non bloccante KO, produzione bloccante OK
void test_produzione_consumazione_bloccante_concorrente_n_OK(){
    clean_suiteBufferN();
    init_suiteBufferN();
    pthread_t pn[6];
    pthread_t cn[7];
    //fase 1
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
    pthread_create(&pn[0], NULL, produzione_bloccante_OK, expected_msg_str);
    pthread_create(&cn[0], NULL, consumazione_bloccante_OK, expected_msg_str);
    pthread_create(&pn[1], NULL, produzione_bloccante_OK, expected_msg_str);
    pthread_create(&pn[2], NULL, produzione_bloccante_OK, expected_msg_str);
    pthread_create(&cn[1], NULL, consumazione_bloccante_OK, expected_msg_str);
    pthread_create(&pn[3], NULL, produzione_bloccante_OK, expected_msg_str);
    pthread_create(&cn[2], NULL, consumazione_bloccante_OK, expected_msg_str);
    pthread_create(&cn[3], NULL, consumazione_bloccante_OK, expected_msg_str);
    pthread_create(&pn[4], NULL, produzione_bloccante_OK, expected_msg_str);
    pthread_create(&cn[4], NULL, consumazione_bloccante_OK, expected_msg_str);
    sleep(3);
    //verifico che il buffer sia stato svuotato dopo l'attesa
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
    //fase 2
    //la consumazione bloccante attenderà un futuro inserimento
    pthread_create(&cn[5], NULL, consumazione_bloccante_OK, go_msg_str);
    pthread_create(&cn[6], NULL, consumazione_non_bloccante_KO, go_msg_str);
    //fase 3
    //inserisco per risvegliare consumazione bloccante
    pthread_create(&pn[5], NULL, produzione_bloccante_OK, go_msg_str);
    sleep(3);
    //verifico che il buffer sia nuovamente vuoto a seguito dell'ultima consumazione risvegliata
    CU_ASSERT_EQUAL(buffer_vuoto->size, 0);
    
    int i;
    for (i=0; i<6; i++) {
        pthread_join(pn[i], NULL);
    }
    for (i=0; i<7; i++) {
        pthread_join(cn[i], NULL);
    }
}

int main(int argc, char *argv[]) {
//    printf("main");
    CU_pSuite pSuite = NULL;
    
    /* inizializza CUnit */
    if (CUE_SUCCESS != CU_initialize_registry())
        return CU_get_error();
    
    /* aggiungi suite test buffer unitario */
    pSuite = CU_add_suite("suiteBufferUnit", init_suiteBufferUnit, clean_suiteBufferUnit);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }
    
    /* aggiungi test buffer unitario */
    if (
        (NULL == CU_add_test(pSuite, "test of test_produzione_non_bloccante_KO_unit", test_produzione_non_bloccante_unit_KO))
        || (NULL == CU_add_test(pSuite, "test of test_consumazione_bloccante_OK_unit", test_consumazione_bloccante_unit_OK))
        || (NULL == CU_add_test(pSuite, "test of test_consumazione_non_bloccante_unit_OK", test_consumazione_non_bloccante_unit_OK))
    ) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* aggiungi suite test buffer ennario */
    pSuite = CU_add_suite("suiteBufferN", init_suiteBufferN, clean_suiteBufferN);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }
    
    /* aggiungi test buffer ennario */
    if (
        (NULL == CU_add_test(pSuite, "test of test_produzione_bloccante_concorrente_n_OK", test_produzione_bloccante_concorrente_n_OK))
        || (NULL == CU_add_test(pSuite, "test of test_produzione_consumazione_bloccante_concorrente_n_OK", test_produzione_consumazione_bloccante_concorrente_n_OK))
        ) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    
    /* esegui tests */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    
    return CU_get_error();
}
