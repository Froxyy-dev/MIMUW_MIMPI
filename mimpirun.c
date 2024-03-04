/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"


int main(int argc, char** argv) {
    const int n = atoi(argv[1]);
    ASSERT_SYS_OK(setenv("MIMPI_SIZE", argv[1], 0));

    const char* prog = argv[2];
    
    for (int i = 0, nr = FIRST_AVAILABLE_DESCRIPTOR; i < n * (n-1); i++, nr += 2) {
        int pipefd[2];
        ASSERT_SYS_OK(channel(pipefd));
        
        ASSERT_SYS_OK(dup2(pipefd[0], nr));
        ASSERT_SYS_OK(close(pipefd[0]));
        
        ASSERT_SYS_OK(dup2(pipefd[1], nr + 1));
        ASSERT_SYS_OK(close(pipefd[1]));
    }

    for (int i = 0; i < n; i++) {
        const pid_t pid = fork();

        if (pid == 0) {
            const pid_t child_pid = getpid();
            char pid_rank[40], rank[12];

            ASSERT_SPRINTF(sprintf(pid_rank, "MIMPI_PID_RANK %d", child_pid));
            ASSERT_SPRINTF(sprintf(rank, "%d", i));

            ASSERT_SYS_OK(setenv(pid_rank, rank, 0));

            for (int receiver = 0; receiver < n; receiver++) {
                for (int sender = 0; sender < n; sender++) {
                    if (receiver == sender) continue;
                    
                    int fd_num = calculate_file_descriptor(n, receiver, sender);
                    
                    if (receiver != i)
                        ASSERT_SYS_OK(close(fd_num));
                    if (sender != i)
                        ASSERT_SYS_OK(close(fd_num + 1));
                }
            }

            ASSERT_SYS_OK(execvp(prog, argv + 2));
        }
    }

    for (int i = 0, nr = FIRST_AVAILABLE_DESCRIPTOR; i < n * (n-1); i++, nr += 2) {
        ASSERT_SYS_OK(close(nr));
        ASSERT_SYS_OK(close(nr + 1));
    }
    
    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }

    ASSERT_SYS_OK(clearenv());

    return 0;
}
