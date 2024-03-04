/**
 * This file is for declarations of  common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#ifndef MIMPI_COMMON_H
#define MIMPI_COMMON_H

#include <assert.h>
#include <stdbool.h>
#include <stdnoreturn.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <pthread.h>

#include "channel.h"

/*
    Assert that expression doesn't evaluate to -1 (as almost every system function does in case of error).

    Use as a function, with a semicolon, like: ASSERT_SYS_OK(close(fd));
    (This is implemented with a 'do { ... } while(0)' block so that it can be used between if () and else.)
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "system command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                  \
    do {                                                                                   \
        int const _errno = (expr);                                                         \
        if (_errno != 0)                                                                   \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

/* Prints (like printf) and quits. */
_Noreturn extern void fatal(const char* fmt, ...);

#define TODO fatal("UNIMPLEMENTED function %s", __PRETTY_FUNCTION__);


/////////////////////////////////////////////
// Put your declarations here

/* Assert that expresion is not null (otherwise memory allocation failed). */
#define ASSERT_MALLOC(expr)                                                                      \
    do {                                                                                         \
        if ((expr) == NULL)                                                                      \
            syserr(                                                                              \
                "Memory allocation failed for %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                              \
            );                                                                                   \
    } while(0)


/* Assert that expression is not negative (otherwise sprintf failed). */
#define ASSERT_SPRINTF(expr)                                                                     \
do {                                                                                             \
    if ((expr) < 0)                                                                              \
        syserr(                                                                                  \
            "Sprintf failed for %s\n\tIn function %s() in %s line %d.\n\tErrno: ",               \
            #expr, __func__, __FILE__, __LINE__                                                  \
        );                                                                                       \
} while(0)


/* First available descriptor */
#define FIRST_AVAILABLE_DESCRIPTOR 20


/// @brief Calculates a file descriptor based on world size, receiver, and sender information.
///
/// @param world_size - total number of processes.
/// @param receiver - rank of the receiving process.
/// @param sender - rank of the sending process.
///
/// @return int:
///     - number of calculated file descriptor.
int calculate_file_descriptor(
    const int world_size, 
    const int receiver, 
    const int sender
);

#endif // MIMPI_COMMON_H