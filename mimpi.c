/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "mimpi_common.h"


/* Return MIMPI_ERROR_NO_SUCH_RANK if rank passed as an argument is not correct. */
#define CHECK_RANK_ERROR(rank)                                                  \
    do {                                                                        \
        int world_size = MIMPI_World_size();                                    \
        if ((rank) < 0 || (rank) >= world_size) {                               \
            return MIMPI_ERROR_NO_SUCH_RANK;                                    \
        }                                                                       \
    } while (0)


/* Return MIMPI_ERROR_ATTEMPTED_SELF_OP if process calls function on itself. */
#define CHECK_SELF_OP_ERROR(rank)                                               \
    do {                                                                        \
        int world_rank = MIMPI_World_rank();                                    \
        if ((rank) == world_rank) {                                             \
            return MIMPI_ERROR_ATTEMPTED_SELF_OP;                               \
        }                                                                       \
    } while (0)


/* Handle MIMPI_ERROR_REMOTE_FINISHED, triggering early return on occurrence. */
#define HANDLE_REMOTE_FINISHED(expr)                                            \
    do {                                                                        \
        if ((expr) == MIMPI_ERROR_REMOTE_FINISHED)                              \
            return MIMPI_ERROR_REMOTE_FINISHED;                                 \
    } while(0)                                                                  \


/* Maximum world size. */
#define MAXSIZE 16

/* Size of metadata used with sending messages. */
#define METADATA_SIZE (2 * sizeof(int)) 


/* Default count used with special messages. */
#define MIMPI_DEFAULT_COUNT -1


/* Default source used with special messages. */
#define MIMPI_DEFAULT_SOURCE -1


/* MAX: Returns the greater of two values. */
#define MAX(a, b) ((a) > (b) ? (a) : (b))


/* MIN: Returns the lesser of two values. */
#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Special tags for internal library communication. */
typedef enum {
    MIMPI_ZERO_TAG = 0,
    MIMPI_DEFAULT_TAG = -1,
    MIMPI_NO_MESSAGE_TAG = -2,
    MIMPI_BROADCAST_TAG = -3,
    MIMPI_DEADLOCK_TAG = -4,
    MIMPI_WAITING_TAG = -5,
    MIMPI_RECEIVED_TAG = -6,
    MIMPI_MAX_TAG = -7,
    MIMPI_MIN_TAG = -8,
    MIMPI_SUM_TAG = -9,
    MIMPI_PROD_TAG = -10,
} MIMPI_Tags;


/* Represents a message */
typedef struct {
    int tag;            // Identifier for the message.
    int count;          // Number of bytes in the message data.
    int source;         // Source process rank.
    void* data;         // Pointer to the message data.
    bool received;      // Flag indicating whether the message has been received.
} Message;


/* Doubly-linked list implementation from slides from course WDP*. */

/* Represents an element in a doubly-linked list, */
typedef struct elem {
    struct elem* next;      // Pointer to the next element in the list.
    struct elem* prev;      // Pointer to the previous element in the list.
    Message* message;       // Pointer to the associated message.
} elem;


/* Represents a doubly-linked list. */
typedef struct list {
    elem* head;         // Pointer to the first element in the list.
    elem* tail;         // Pointer to the last element in the list.
} list;


/*  Represents the result of a read operation */
typedef struct Readcode {
    bool error_occured;     // Flag indicating whether an error occurred during the read operation.
    void* data;             // Pointer to the data obtained from the read operation.
} Readcode; 


Message const MIMPI_DEFAULT_MSG = {
    .tag = MIMPI_DEFAULT_TAG, .count = MIMPI_DEFAULT_COUNT, .source = MIMPI_DEFAULT_SOURCE, .data = NULL, .received = false
};
Message* MIMPI_waiting;

bool MIMPI_deadlock_enabled;
bool MIMPI_already_left[MAXSIZE];

list* MIMPI_send_not_received;
list* MIMPI_others_recv[MAXSIZE];
list* MIMPI_received_messages[MAXSIZE];

pthread_cond_t MIMPI_cond;
pthread_mutex_t MIMPI_mutex;
pthread_t MIMPI_threads[MAXSIZE];


/// @brief Creates a message.
///
/// @param tag - identifier for the message.
/// @param count - number of bytes in the message data.
/// @param source - source process rank.
/// @param data - pointer to the message data.
///
/// @return Message*:
///     - pointer to the newly created message.
static Message* create_message(
    int tag, 
    int count, 
    int source, 
    void* data
) {
    Message* message = (Message*)malloc(sizeof(Message));
    ASSERT_MALLOC(message);

    *message = (Message) {.tag = tag, .count = count, .source = source, .data = data, .received = false};
    return message;
}


/// @brief Deletes a message.
///
/// @param message - pointer to the message to be deleted.
static void delete_message(
    Message* message
) {
    if(message->tag != MIMPI_DEADLOCK_TAG && message->tag != MIMPI_NO_MESSAGE_TAG)
        free(message->data);
    message->data = NULL;
    free(message);
}


/// @brief Compares two messages.
///
/// @param a - pointer to the first message.
/// @param b - pointer to the second message.
///
/// @return bool:
///     - true if the messages are equal, false otherwise.
static bool compare_message(
    const Message* a, 
    const Message* b
) {
    return a->source == b->source && a->count == b->count && (b->tag == MIMPI_ANY_TAG || a->tag == b->tag);
}


/// @brief Creates an element.
///
/// @param next - pointer to the next element in the list.
/// @param prev - pointer to the previous element in the list.
/// @param message - pointer to the associated message.
///
/// @return elem*:
///     - pointer to the newly created element.
static elem* create_elem(
    elem* next, 
    elem* prev, 
    Message* message
) {
    elem* el = (elem*)malloc(sizeof(elem));
    ASSERT_MALLOC(el);

    *el = (elem) {.next = next, .prev = prev, .message = message};
    return el;
}


/// @brief Deletes an element.
///
/// @param el - pointer to the element to be deleted.
static void delete_elem(
    elem* el
) {
    el->next = NULL;
    el->prev = NULL;
    if (el->message)
        delete_message(el->message);
    el->message = NULL;
    free(el);
}


/// @brief Creates a list with empty head and tail elements.
///
/// @return list*:
///     - pointer to the newly created list.
static list* create_list() {
    list* l = (list*)malloc(sizeof(list));
    ASSERT_MALLOC(l);
    
    *l = (list) {
        .head = create_elem(NULL, NULL, NULL), 
        .tail = create_elem(NULL, NULL, NULL)
    };

    l->tail->next = l->head;
    l->head->prev = l->tail;

    return l;
}


/// @brief Removes an element from the list.
///
/// @param el - pointer to the element to be removed from the list.
static void remove_from_list(
    elem* el
) {
    elem* previous_element = el->prev;
    elem* next_element = el->next;

    if (previous_element)
        previous_element->next = next_element;

    if (next_element)
        next_element->prev = previous_element;

    delete_elem(el);
}


/// @brief Deletes a list.
///
/// @param l - pointer to the list to be deleted.
static void delete_list(
    list* l
) {
    elem* current = l->tail;

    while (current) {
        elem* next = current->next;
        remove_from_list(current);
        current = next;
    }

    l->tail = NULL;
    l->head = NULL;
    free(l);
}


/// @brief Finds an element in the list.
///
/// @param l - pointer to the list.
/// @param to_compare - pointer to the message to compare.
///
/// @return elem*:
///     - pointer to the found element, head of a list otherwise.
static elem* find_elem_in_list(
    list* l,
    Message* to_compare
) {
    elem* current = l->tail->next;
            
    while (current->message != NULL && current != l->head) {
        if (compare_message(current->message, to_compare)) {
            break;
        }
        
        current = current->next;
    }

    return current;
}


/// @brief Adds an element to the front of the list.
///
/// @param l - pointer to the list.
/// @param el - pointer to the element to be added.
static void push_front(
    list* l, 
    elem* el
) {
    elem* last = l->head->prev;
    last->next = el;

    el->prev = last;
    el->next = l->head;

    l->head->prev = el;
}


/// @brief Reads data from a channel.
///
/// @param fd - file descriptor of the channel.
/// @param count - number of bytes to read.
///
/// @return Readcode*:
///     - pointer to the Readcode containing read data and error status.
static Readcode* read_from_channel(
    int fd, 
    size_t count
) {
    void* data = (void*)malloc(sizeof(void) * count);
    ASSERT_MALLOC(data);

    Readcode* readcode = (Readcode*)malloc(sizeof(Readcode));
    ASSERT_MALLOC(readcode);

    readcode->error_occured = false;

    int bytes_read = 0;
    
    while (bytes_read < count) {
        int current_read = chrecv(fd, data + bytes_read, count - bytes_read);

        if (current_read <= 0) {
            readcode->error_occured = true;
            break;
        }

        bytes_read += current_read;
    }

    readcode->data = data;
    return readcode;
}

/// @brief Writes data to a channel.
///
/// @param fd - file descriptor of the channel.
/// @param count - number of bytes to write.
/// @param data - pointer to the data to be written.
///
/// @return bool:
///     - true if the write was successful, false otherwise.
static bool write_to_channel(
    int fd, 
    size_t count, 
    const void* data
) {
    int bytes_wrote = 0;
    
    while (bytes_wrote < count) {
        int current_wrote = chsend(fd, data + bytes_wrote, count - bytes_wrote);
        
        if (current_wrote <= 0) {
            return false;
        }
    
        bytes_wrote += current_wrote;
    }

    return true;
}


/// @brief Handles a reduce operation based on the tag.
///
/// @param received_data - pointer to the data received.
/// @param count - number of bytes in the data.
/// @param tag - operation tag specified in MIMPI_Tags.
/// @param data - pointer to the data to be applied in the reduce operation.
static void handle_reduce_operation(
    void* received_data,
    int count,
    int tag,
    void* data
) {
    u_int8_t* dataInt = (u_int8_t*)data;
    u_int8_t* messageDataInt = (u_int8_t*)received_data;

    if (tag == MIMPI_MAX_TAG) {
        for (int i = 0; i < count; i++) {
            dataInt[i] = MAX(dataInt[i], messageDataInt[i]);
        }
    }
    else if (tag == MIMPI_MIN_TAG) {
        for (int i = 0; i < count; i++) {
            dataInt[i] = MIN(dataInt[i], messageDataInt[i]);
        }   
    }
    else if (tag == MIMPI_SUM_TAG) {
        for (int i = 0; i < count; i++) {
            dataInt[i] += messageDataInt[i];
        }
    }
    else {
        for (int i = 0; i < count; i++) {
            dataInt[i] *= messageDataInt[i];
        }
    }

    memcpy(data, dataInt, count);
}


/// @brief Determines the power of 2 corresponding to the given rank.
///
/// @param rank - rank of the process.
///
/// @return int:
///     - power of 2 associated with the rank.
static int get_power(
    int rank
) {
    if (rank <= 1) return rank;
    if (rank <= 3) return 2;
    if (rank <= 7) return 4;
    return 8;
}


/// @brief Handles communication loop for group functions.
///
/// @param data - pointer to the data for communication.
/// @param count - number of bytes in the data.
/// @param root - rank of the root process.
/// @param tag - identifier to be passed in communication.
/// @param world_rank - rank of the current process.
/// @param world_size - total number of processes.
/// @param begin - flag indicating the start of the communication loop.
///
/// @return MIMPI_Retcode:
///     - MIMPI_SUCCESS on successful completion, an error code otherwise.
static MIMPI_Retcode communication_loop(
    void* data, 
    int count, 
    int root, 
    int tag, 
    int world_rank, 
    int world_size, 
    bool begin
) { 
    int receive_from = world_rank - get_power(world_rank);
    int power = get_power(world_rank) * 2;
    int start_from = world_rank + power;

    if (world_rank == root) {
        power = 1;
        start_from = 1;
    }
    else if (world_rank == 0) {
        receive_from = root - get_power(root);
        power = get_power(root) * 2;
        start_from = root + power;
    }

    if (begin) {
        while (start_from < world_size) {
            int helper = (start_from == root) ? 0 : start_from;

            HANDLE_REMOTE_FINISHED(MIMPI_Recv(data, count, helper, tag));

            start_from += power;
            power *= 2;
        }
        
        if (world_rank != root) {
            if (receive_from == root) {
                receive_from = 0;
            } 
            else if (receive_from == 0) {
                receive_from = root;
            }

            HANDLE_REMOTE_FINISHED(MIMPI_Send(data, count, receive_from, tag));
        }
    }
    else {
        if (world_rank != root) {
            if (receive_from == root) {
                receive_from = 0;
            } 
            else if (receive_from == 0) {
                receive_from = root;
            }

            HANDLE_REMOTE_FINISHED(MIMPI_Recv(data, count, receive_from, tag));
        }

        while (start_from < world_size) {
            int helper = (start_from == root) ? 0 : start_from;

            HANDLE_REMOTE_FINISHED(MIMPI_Send(data, count, helper, tag));

            start_from += power;
            power *= 2;
        }
    }

    return MIMPI_SUCCESS;
}


/// @brief Handles communication through a channel in a helper thread.
///
/// @param data - pointer to the data containing the sender's rank.
static void* handle_channel(
    void* data
) {
    const int sender = *((int*)data);
    free(data);
    
    const int receiver = MIMPI_World_rank();
    const int world_size = MIMPI_World_size();
    const int fd_num = calculate_file_descriptor(world_size, receiver, sender);
    int count, tag;

    while(true) {                
        Readcode* readcode = read_from_channel(fd_num, METADATA_SIZE);

        if (readcode->error_occured) {
            ASSERT_ZERO(pthread_mutex_lock(&MIMPI_mutex));
            
            MIMPI_already_left[sender] = true;
            if (MIMPI_waiting->source == sender) {
                ASSERT_ZERO(pthread_cond_signal(&MIMPI_cond));
            }

            if (readcode->data)
                free(readcode->data);
                
            readcode->data = NULL;
            free(readcode);
            ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
            break;
        }
        else {
            void* info = readcode->data;
            memcpy(&count, info, sizeof(int));
            memcpy(&tag, info + sizeof(int), sizeof(int));
            free(info);

            readcode->data = NULL;
            free(readcode);
        }

        void* message_data = NULL;
        
        if (tag != MIMPI_NO_MESSAGE_TAG && tag != MIMPI_DEADLOCK_TAG) {
            readcode = read_from_channel(fd_num, count);
            message_data = readcode->data;

            readcode->data = NULL;
            free(readcode);
        }
        
        bool waiting_tag = false, receive_tag = false;

        if (tag == MIMPI_WAITING_TAG || tag == MIMPI_RECEIVED_TAG) {
            if (tag == MIMPI_WAITING_TAG) {
                waiting_tag = true;
            }
            else if (tag == MIMPI_RECEIVED_TAG) {
                receive_tag = true;
            }

            memcpy(&count, message_data, sizeof(int));
            memcpy(&tag, message_data + sizeof(int), sizeof(int));
        }

        Message *message = create_message(tag, count, sender, message_data);
        elem *el = create_elem(NULL, NULL, message);

        ASSERT_ZERO(pthread_mutex_lock(&MIMPI_mutex));
        
        if (tag == MIMPI_DEADLOCK_TAG) {
            MIMPI_waiting->tag = MIMPI_DEADLOCK_TAG;
            MIMPI_waiting->received = true;
            
            push_front(MIMPI_others_recv[sender], el);

            ASSERT_ZERO(pthread_cond_signal(&MIMPI_cond));
        }
        else if (waiting_tag) {
            elem* elem_found = find_elem_in_list(MIMPI_send_not_received, message);

            if (elem_found == MIMPI_send_not_received->head) {
                push_front(MIMPI_others_recv[sender], el);

                if (MIMPI_waiting->source == sender && MIMPI_waiting->received == false) {
                    MIMPI_waiting->received = true;
                    MIMPI_waiting->tag = MIMPI_DEADLOCK_TAG;

                    ASSERT_ZERO(pthread_cond_signal(&MIMPI_cond));
                }
            }
            else {
                delete_elem(el);
            }
        }
        else if (receive_tag) {
            remove_from_list(find_elem_in_list(MIMPI_send_not_received, message));
            delete_elem(el);
        }
        else {
            push_front(MIMPI_received_messages[sender], el);

            if (compare_message(message, MIMPI_waiting)) {
                *MIMPI_waiting = *message;
                MIMPI_waiting->received = true;

                ASSERT_ZERO(pthread_cond_signal(&MIMPI_cond));
            }
        }

        ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
    }

    ASSERT_SYS_OK(close(fd_num));
    return NULL;
}   


void MIMPI_Init(
    bool enable_deadlock_detection
) {
    channels_init();

    MIMPI_deadlock_enabled = enable_deadlock_detection;

    const int world_size = MIMPI_World_size();
    const int world_rank = MIMPI_World_rank();

    MIMPI_waiting = (Message*)malloc(sizeof(Message));
    ASSERT_MALLOC(MIMPI_waiting);
    *MIMPI_waiting = MIMPI_DEFAULT_MSG;

    if (enable_deadlock_detection) {
        for (int i = 0; i < world_size; i++) {
            if (i == world_rank) continue;

            MIMPI_others_recv[i] = create_list();
        }

        MIMPI_send_not_received = create_list();
    }

    for (int i = 0; i < world_size; i++) {
        if (i == world_rank) continue;

        MIMPI_received_messages[i] = create_list();       
    }

    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE));

    ASSERT_ZERO(pthread_mutex_init(&MIMPI_mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&MIMPI_cond, NULL));

    for (int worker = 0; worker < world_size; worker++) {
        if (worker == world_rank) {
            continue;
        }

        int* worker_id = malloc(sizeof(int));
        ASSERT_MALLOC(worker_id);

        *worker_id = worker;
        ASSERT_ZERO(pthread_create(&MIMPI_threads[worker], &attr, handle_channel, worker_id));
    }

    ASSERT_ZERO(pthread_attr_destroy(&attr));
}


void MIMPI_Finalize() {
    const int world_size = MIMPI_World_size();
    const int world_rank = MIMPI_World_rank();

    for (int i = 0; i < world_size; i++) {
        if (i == world_rank) continue;

        int fd_num = calculate_file_descriptor(world_size, i, world_rank) + 1;
        ASSERT_SYS_OK(close(fd_num));
    }
    
    for (int worker = 0; worker < world_size; worker++) {
        if (worker == world_rank) continue;

        ASSERT_ZERO(pthread_join(MIMPI_threads[worker], NULL));
    }

    channels_finalize();

    delete_message(MIMPI_waiting);

    for (int i = 0; i < world_size; i++) {
        if (i == world_rank) continue;

        delete_list(MIMPI_received_messages[i]);
    }

    if (MIMPI_deadlock_enabled) {
        delete_list(MIMPI_send_not_received);

        for (int i = 0; i < world_size; i++) {
            if (i == world_rank) continue;

            delete_list(MIMPI_others_recv[i]);
        }
    }

    ASSERT_ZERO(pthread_cond_destroy(&MIMPI_cond));
    ASSERT_ZERO(pthread_mutex_destroy(&MIMPI_mutex));
}


int MIMPI_World_size() {
    return atoi(getenv("MIMPI_SIZE"));
}


int MIMPI_World_rank() {
    const pid_t pid = getpid();
    char pid_rank[40];
    ASSERT_SPRINTF(sprintf(pid_rank, "MIMPI_PID_RANK %d", pid));

    return atoi(getenv(pid_rank));
}


MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    CHECK_RANK_ERROR(destination);
    CHECK_SELF_OP_ERROR(destination);
    
    const int world_size = MIMPI_World_size();
    const int sender = MIMPI_World_rank();
    const int fd_num = calculate_file_descriptor(world_size, destination, sender) + 1;

    if (MIMPI_deadlock_enabled && tag >= MIMPI_ANY_TAG) {
        ASSERT_ZERO(pthread_mutex_lock(&MIMPI_mutex));

        Message* msg = MIMPI_others_recv[destination]->tail->next->message;

        if (msg != NULL && msg->count == count && msg->tag == tag) {
            remove_from_list(MIMPI_others_recv[destination]->tail->next);
        }

        Message* message = create_message(tag, count, destination, NULL);
        elem* el = create_elem(NULL, NULL, message);
        push_front(MIMPI_send_not_received, el);

        ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
    }

    void* to_send = NULL;
    size_t bytes_to_send;

    if (tag != MIMPI_NO_MESSAGE_TAG && tag != MIMPI_DEADLOCK_TAG) {
        to_send = (void*)malloc(sizeof(void) * ((size_t)count + METADATA_SIZE));
        ASSERT_MALLOC(to_send);

        memcpy(to_send, &count, sizeof(int));
        memcpy(to_send + sizeof(int), &tag, sizeof(int));
        memcpy(to_send + METADATA_SIZE, data, count);
        bytes_to_send = (size_t)count + METADATA_SIZE;
    }
    else {
        to_send = (void*)malloc(sizeof(void) * METADATA_SIZE);
        ASSERT_MALLOC(to_send);

        memcpy(to_send, &count, sizeof(int));
        memcpy(to_send + sizeof(int), &tag, sizeof(int));
        bytes_to_send = METADATA_SIZE;
    }

    if (!write_to_channel(fd_num, bytes_to_send, to_send)) {
        free(to_send);

        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    free(to_send);

    return MIMPI_SUCCESS;
}


MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    CHECK_RANK_ERROR(source);
    CHECK_SELF_OP_ERROR(source);

    ASSERT_ZERO(pthread_mutex_lock(&MIMPI_mutex));

    Message* to_compare = create_message(tag, count, source, data);
    elem* elem_found = find_elem_in_list(MIMPI_received_messages[source], to_compare);
    free(to_compare);

    void* to_receive = NULL;

    if (elem_found->message == NULL || elem_found == MIMPI_received_messages[source]->head) {
        *MIMPI_waiting = (Message) {
            .tag = tag, .count = count, .source = source, .data = NULL, .received = false
        };

        if (MIMPI_deadlock_enabled && tag >= MIMPI_ANY_TAG) {
            elem* first_on_list = MIMPI_others_recv[source]->tail->next;
            Message* msg = first_on_list->message;

            if (msg != NULL && msg->tag >= MIMPI_ANY_TAG) {
                *MIMPI_waiting = MIMPI_DEFAULT_MSG;
                remove_from_list(first_on_list);

                MIMPI_Send(NULL, MIMPI_DEFAULT_COUNT, source, MIMPI_DEADLOCK_TAG);

                ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
                return MIMPI_ERROR_DEADLOCK_DETECTED;   
            }

            void* info = (void*)malloc(sizeof(void) * METADATA_SIZE);
            ASSERT_MALLOC(info);

            memcpy(info, &count, sizeof(int));
            memcpy(info + sizeof(int), &tag, sizeof(int));
                    
            if (MIMPI_Send(info, METADATA_SIZE, source, MIMPI_WAITING_TAG) == MIMPI_ERROR_REMOTE_FINISHED) {
                free(info);
                *MIMPI_waiting = MIMPI_DEFAULT_MSG;
                remove_from_list(MIMPI_others_recv[source]->tail->next);

                ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            free(info);
        }
        
        while (!MIMPI_waiting->received && !MIMPI_already_left[source]) {
            ASSERT_ZERO(pthread_cond_wait(&MIMPI_cond, &MIMPI_mutex));
        }

        if (MIMPI_waiting->tag == MIMPI_DEADLOCK_TAG) {
            *MIMPI_waiting = MIMPI_DEFAULT_MSG;
            remove_from_list(MIMPI_others_recv[source]->tail->next);

            ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }

        if (MIMPI_already_left[source] && !MIMPI_waiting->received) {
            *MIMPI_waiting = MIMPI_DEFAULT_MSG;
            ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        to_compare = create_message(tag, count, source, data);
        elem* elem_received = find_elem_in_list(MIMPI_received_messages[source], to_compare);
        
        if (tag != MIMPI_NO_MESSAGE_TAG) {
            to_receive = (void*)malloc(sizeof(void) * count);
            ASSERT_MALLOC(to_receive);

            memcpy(to_receive, elem_received->message->data, count);
        }

        remove_from_list(elem_received);        
        free(to_compare);

        *MIMPI_waiting = MIMPI_DEFAULT_MSG;
    }
    else {
        if (tag != MIMPI_NO_MESSAGE_TAG && tag != MIMPI_DEADLOCK_TAG) {
            to_receive = (void*)malloc(sizeof(void) * count);
            ASSERT_MALLOC(to_receive);

            memcpy(to_receive, elem_found->message->data, count);
        }

        remove_from_list(elem_found);
    }   

    if (MIMPI_deadlock_enabled && tag >= MIMPI_ANY_TAG) {
        void* info = (void*)malloc(sizeof(void) * METADATA_SIZE);
        ASSERT_MALLOC(info);

        memcpy(info, &count, sizeof(int));
        memcpy(info + sizeof(int), &tag, sizeof(int));

        MIMPI_Send(info, METADATA_SIZE, source, MIMPI_RECEIVED_TAG);
        free(info);
    }

    ASSERT_ZERO(pthread_mutex_unlock(&MIMPI_mutex));

    if (tag <= MIMPI_MAX_TAG) {
        handle_reduce_operation(to_receive, count, tag, data);
    }
    else if (tag != MIMPI_NO_MESSAGE_TAG) {
        memcpy(data, to_receive, count);
    }

    if (to_receive)
        free(to_receive);

    return MIMPI_SUCCESS;
}


MIMPI_Retcode MIMPI_Barrier() {
    const int world_rank = MIMPI_World_rank();
    const int world_size = MIMPI_World_size(); 

    HANDLE_REMOTE_FINISHED(communication_loop(NULL, MIMPI_DEFAULT_COUNT, 0, MIMPI_NO_MESSAGE_TAG, world_rank, world_size, true));

    return communication_loop(NULL, MIMPI_DEFAULT_COUNT, 0, MIMPI_NO_MESSAGE_TAG, world_rank, world_size, false);
}


MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    CHECK_RANK_ERROR(root);

    const int world_rank = MIMPI_World_rank();
    const int world_size = MIMPI_World_size();  

    HANDLE_REMOTE_FINISHED(communication_loop(NULL, MIMPI_DEFAULT_COUNT, root, MIMPI_NO_MESSAGE_TAG, world_rank, world_size, true));

    return communication_loop(data, count, root, MIMPI_BROADCAST_TAG, world_rank, world_size, false);
}


MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    CHECK_RANK_ERROR(root);

    const int world_rank = MIMPI_World_rank();
    const int world_size = MIMPI_World_size();

    void* memory = (void*)malloc(sizeof(void) * count);
    ASSERT_MALLOC(memory);

    memcpy(memory, send_data, count);

    if (communication_loop(memory, count, root, MIMPI_MAX_TAG - op, world_rank, world_size, true) == MIMPI_ERROR_REMOTE_FINISHED) {
        free(memory);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    if (world_rank == root) {
        memcpy(recv_data, memory, count);
    }

    free(memory);

    return communication_loop(NULL, MIMPI_DEFAULT_COUNT, root, MIMPI_NO_MESSAGE_TAG, world_rank, world_size, false);
}