/*
 * engine.c - Multi-Container Runtime Supervisor + CLI
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <sched.h>

#include "monitor_ioctl.h"

#define SOCKET_PATH     "/tmp/engine.sock"
#define LOG_DIR         "/tmp/engine_logs"
#define MAX_CONTAINERS  32
#define MAX_ID_LEN      64
#define MAX_CMD_LEN     512
#define MAX_RSP_LEN     4096
#define LOG_BUF_SIZE    256
#define LOG_LINE_MAX    512
#define DEFAULT_SOFT_MIB  40
#define DEFAULT_HARD_MIB  64

typedef struct {
    char   container_id[MAX_ID_LEN];
    char   line[LOG_LINE_MAX];
} LogEntry;

typedef struct {
    LogEntry        entries[LOG_BUF_SIZE];
    int             head;
    int             tail;
    int             count;
    int             done;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} BoundedBuffer;

static BoundedBuffer g_log_buf;

static void log_buf_init(BoundedBuffer *b) {
    b->head = 0;
    b->tail = 0;
    b->count = 0;
    b->done = 0;
    pthread_mutex_init(&b->lock, NULL);
    pthread_cond_init(&b->not_empty, NULL);
    pthread_cond_init(&b->not_full, NULL);
}

static void log_buf_push(BoundedBuffer *b, const char *id, const char *line) {
    pthread_mutex_lock(&b->lock);
    while (b->count == LOG_BUF_SIZE && !b->done) {
        pthread_cond_wait(&b->not_full, &b->lock);
    }
    if (!b->done && b->count < LOG_BUF_SIZE) {
        LogEntry *e = &b->entries[b->tail];
        strncpy(e->container_id, id, MAX_ID_LEN - 1);
        e->container_id[MAX_ID_LEN - 1] = '\0';
        strncpy(e->line, line, LOG_LINE_MAX - 1);
        e->line[LOG_LINE_MAX - 1] = '\0';
        b->tail = (b->tail + 1) % LOG_BUF_SIZE;
        b->count++;
        pthread_cond_signal(&b->not_empty);
    }
    pthread_mutex_unlock(&b->lock);
}

static int log_buf_pop(BoundedBuffer *b, LogEntry *out) {
    pthread_mutex_lock(&b->lock);
    while (b->count == 0 && !b->done) {
        pthread_cond_wait(&b->not_empty, &b->lock);
    }
    if (b->count == 0) {
        pthread_mutex_unlock(&b->lock);
        return 0;
    }
    *out = b->entries[b->head];
    b->head = (b->head + 1) % LOG_BUF_SIZE;
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->lock);
    return 1;
}

typedef enum {
    STATE_STARTING,
    STATE_RUNNING,
    STATE_STOPPED,
    STATE_KILLED,
    STATE_LIMIT_KILLED
} ContainerState;

static const char *state_str(ContainerState s) {
    switch (s) {
        case STATE_STARTING: return "STARTING";
        case STATE_RUNNING: return "RUNNING";
        case STATE_STOPPED: return "STOPPED";
        case STATE_KILLED: return "KILLED";
        case STATE_LIMIT_KILLED: return "LIMIT_KILLED";
        default: return "UNKNOWN";
    }
}

typedef struct {
    char            id[MAX_ID_LEN];
    pid_t           pid;
    time_t          start_time;
    ContainerState  state;
    unsigned long   soft_mib;
    unsigned long   hard_mib;
    int             nice_val;
    char            log_path[256];
    int             exit_code;
    int             stop_requested;
    int             pipe_stdout[2];
    int             pipe_stderr[2];
    int             run_client_fd;
} Container;

static Container g_containers[MAX_CONTAINERS];
static int g_num_containers = 0;
static pthread_mutex_t g_meta_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile int g_shutdown = 0;
static int g_monitor_fd = -1;
static pthread_t g_consumer_tid;

static void monitor_register(const char *id, pid_t pid,
                             unsigned long soft_mib, unsigned long hard_mib) {
    if (g_monitor_fd < 0) return;
    
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    req.pid = pid;
    req.soft_limit_bytes = soft_mib * 1024 * 1024;
    req.hard_limit_bytes = hard_mib * 1024 * 1024;
    ioctl(g_monitor_fd, MONITOR_REGISTER, &req);
}

static void monitor_unregister(const char *id, pid_t pid) {
    if (g_monitor_fd < 0) return;
    
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    req.pid = pid;
    ioctl(g_monitor_fd, MONITOR_UNREGISTER, &req);
}

static void *consumer_thread_fn(void *arg) {
    (void)arg;
    LogEntry e;
    
    while (log_buf_pop(&g_log_buf, &e)) {
        char path[300];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, e.container_id);
        
        FILE *f = fopen(path, "a");
        if (f) {
            time_t t = time(NULL);
            char ts[32];
            struct tm *tm_info = localtime(&t);
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm_info);
            fprintf(f, "[%s] %s\n", ts, e.line);
            fclose(f);
        }
    }
    return NULL;
}

typedef struct {
    int fd;
    char id[MAX_ID_LEN];
} ProducerArg;

static void *producer_thread_fn(void *arg) {
    ProducerArg *pa = (ProducerArg *)arg;
    char buf[LOG_LINE_MAX];
    char line[LOG_LINE_MAX];
    int pos = 0;
    
    while (1) {
        ssize_t n = read(pa->fd, buf, sizeof(buf) - 1);
        if (n <= 0) {
            if (pos > 0) {
                line[pos] = '\0';
                log_buf_push(&g_log_buf, pa->id, line);
            }
            break;
        }
        buf[n] = '\0';
        
        for (ssize_t i = 0; i < n; i++) {
            if (buf[i] == '\n' || pos == LOG_LINE_MAX - 1) {
                line[pos] = '\0';
                if (pos > 0) {
                    log_buf_push(&g_log_buf, pa->id, line);
                }
                pos = 0;
            } else {
                line[pos++] = buf[i];
            }
        }
    }
    close(pa->fd);
    free(pa);
    return NULL;
}

typedef struct {
    char rootfs[256];
    char command[256];
    int nice_val;
    int stdout_fd;
    int stderr_fd;
} CloneArgs;

#define CLONE_STACK_SIZE (1024 * 1024)

static int container_fn(void *arg) {
    CloneArgs *ca = (CloneArgs *)arg;
    
    dup2(ca->stdout_fd, STDOUT_FILENO);
    dup2(ca->stderr_fd, STDERR_FILENO);
    close(ca->stdout_fd);
    close(ca->stderr_fd);
    
    if (ca->nice_val != 0) {
        nice(ca->nice_val);
    }
    
    if (chroot(ca->rootfs) != 0) {
        return 1;
    }
    chdir("/");
    
    mkdir("/proc", 0555);
    mount("proc", "/proc", "proc", 0, NULL);
    
    execl("/bin/sh", "sh", "-c", ca->command, (char *)NULL);
    return 1;
}

/* Reaper thread: blocking waitpid loop - avoids SIGCHLD race where
 * handler fires before run_client_fd is stored in the container struct */
static void *reaper_thread_fn(void *arg) {
    (void)arg;
    while (!g_shutdown) {
        int status;
        pid_t pid = waitpid(-1, &status, 0);
        if (pid <= 0) {
            if (errno == EINTR) continue;
            if (errno == ECHILD) { usleep(100000); continue; }
            break;
        }

        pthread_mutex_lock(&g_meta_lock);
        for (int i = 0; i < g_num_containers; i++) {
            Container *c = &g_containers[i];
            if (c->pid != pid) continue;

            if (WIFEXITED(status)) {
                c->exit_code = WEXITSTATUS(status);
                c->state = STATE_STOPPED;
            } else if (WIFSIGNALED(status)) {
                int sig_num = WTERMSIG(status);
                c->exit_code = 128 + sig_num;
                if (sig_num == SIGKILL && !c->stop_requested)
                    c->state = STATE_LIMIT_KILLED;
                else
                    c->state = STATE_KILLED;
            }

            if (c->run_client_fd >= 0) {
                char rsp[64];
                snprintf(rsp, sizeof(rsp), "EXIT %d\n", c->exit_code);
                write(c->run_client_fd, rsp, strlen(rsp));
                close(c->run_client_fd);
                c->run_client_fd = -1;
            }

            monitor_unregister(c->id, pid);
            break;
        }
        pthread_mutex_unlock(&g_meta_lock);
    }
    return NULL;
}

static void sigchld_handler(int sig) {
    (void)sig; /* reaper thread handles actual reaping */
}

static void sigterm_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

static int launch_container(const char *id, const char *rootfs,
                            const char *command,
                            unsigned long soft_mib, unsigned long hard_mib,
                            int nice_val, char *err_out, int run_client_fd) {
    
    pthread_mutex_lock(&g_meta_lock);
    
    for (int i = 0; i < g_num_containers; i++) {
        if (strcmp(g_containers[i].id, id) == 0 && 
            g_containers[i].state == STATE_RUNNING) {
            pthread_mutex_unlock(&g_meta_lock);
            snprintf(err_out, MAX_RSP_LEN, "ERROR container '%s' already running", id);
            return -1;
        }
    }
    
    if (g_num_containers >= MAX_CONTAINERS) {
        pthread_mutex_unlock(&g_meta_lock);
        snprintf(err_out, MAX_RSP_LEN, "ERROR max containers reached");
        return -1;
    }
    
    Container *c = &g_containers[g_num_containers];
    memset(c, 0, sizeof(*c));
    strncpy(c->id, id, MAX_ID_LEN - 1);
    c->id[MAX_ID_LEN - 1] = '\0';
    c->soft_mib = soft_mib;
    c->hard_mib = hard_mib;
    c->nice_val = nice_val;
    c->state = STATE_STARTING;
    c->start_time = time(NULL);
    c->stop_requested = 0;
    c->run_client_fd = run_client_fd;
    snprintf(c->log_path, sizeof(c->log_path), "%s/%s.log", LOG_DIR, id);
    
    if (pipe(c->pipe_stdout) < 0 || pipe(c->pipe_stderr) < 0) {
        pthread_mutex_unlock(&g_meta_lock);
        snprintf(err_out, MAX_RSP_LEN, "ERROR pipe failed");
        return -1;
    }
    
    CloneArgs *ca = malloc(sizeof(CloneArgs));
    strncpy(ca->rootfs, rootfs, 255);
    strncpy(ca->command, command, 255);
    ca->nice_val = nice_val;
    ca->stdout_fd = c->pipe_stdout[1];
    ca->stderr_fd = c->pipe_stderr[1];
    
    char *stack = malloc(CLONE_STACK_SIZE);
    if (!stack) {
        free(ca);
        pthread_mutex_unlock(&g_meta_lock);
        snprintf(err_out, MAX_RSP_LEN, "ERROR malloc failed");
        return -1;
    }
    char *stack_top = stack + CLONE_STACK_SIZE;
    
    int clone_flags = CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWUTS | SIGCHLD;
    pid_t pid = clone(container_fn, stack_top, clone_flags, ca);
    
    free(stack);
    free(ca);
    
    if (pid < 0) {
        close(c->pipe_stdout[0]);
        close(c->pipe_stdout[1]);
        close(c->pipe_stderr[0]);
        close(c->pipe_stderr[1]);
        pthread_mutex_unlock(&g_meta_lock);
        snprintf(err_out, MAX_RSP_LEN, "ERROR clone failed: %s", strerror(errno));
        return -1;
    }
    
    c->pid = pid;
    c->state = STATE_RUNNING;
    
    close(c->pipe_stdout[1]);
    close(c->pipe_stderr[1]);
    
    ProducerArg *pa_out = malloc(sizeof(ProducerArg));
    pa_out->fd = c->pipe_stdout[0];
    strncpy(pa_out->id, id, MAX_ID_LEN - 1);
    pthread_t pt_out;
    pthread_create(&pt_out, NULL, producer_thread_fn, pa_out);
    pthread_detach(pt_out);
    
    ProducerArg *pa_err = malloc(sizeof(ProducerArg));
    pa_err->fd = c->pipe_stderr[0];
    strncpy(pa_err->id, id, MAX_ID_LEN - 1);
    pthread_t pt_err;
    pthread_create(&pt_err, NULL, producer_thread_fn, pa_err);
    pthread_detach(pt_err);
    
    g_num_containers++;
    pthread_mutex_unlock(&g_meta_lock);
    
    monitor_register(id, pid, soft_mib, hard_mib);
    
    return pid;
}

static void handle_start(char *args, char *response, int run_client_fd) {
    char id[MAX_ID_LEN] = {0};
    char rootfs[256] = {0};
    char command[256] = {0};
    unsigned long soft = DEFAULT_SOFT_MIB;
    unsigned long hard = DEFAULT_HARD_MIB;
    int nice_val = 0;
    
    char buf[MAX_CMD_LEN];
    strncpy(buf, args, MAX_CMD_LEN - 1);
    char *tokens[32];
    int ntok = 0;
    char *tok = strtok(buf, " \t\n");
    while (tok && ntok < 32) {
        tokens[ntok++] = tok;
        tok = strtok(NULL, " \t\n");
    }
    
    if (ntok < 3) {
        snprintf(response, MAX_RSP_LEN, "ERROR: need <id> <rootfs> <command>");
        return;
    }
    
    strncpy(id, tokens[0], MAX_ID_LEN - 1);
    strncpy(rootfs, tokens[1], 255);
    strncpy(command, tokens[2], 255);
    
    for (int i = 3; i < ntok; i++) {
        if (strcmp(tokens[i], "--soft-mib") == 0 && i + 1 < ntok) {
            soft = strtoul(tokens[++i], NULL, 10);
        } else if (strcmp(tokens[i], "--hard-mib") == 0 && i + 1 < ntok) {
            hard = strtoul(tokens[++i], NULL, 10);
        } else if (strcmp(tokens[i], "--nice") == 0 && i + 1 < ntok) {
            nice_val = atoi(tokens[++i]);
        }
    }
    
    char err[MAX_RSP_LEN] = {0};
    int pid = launch_container(id, rootfs, command, soft, hard, nice_val, err, run_client_fd);
    
    if (pid < 0) {
        strncpy(response, err, MAX_RSP_LEN - 1);
    } else if (run_client_fd >= 0) {
        snprintf(response, MAX_RSP_LEN, "__RUN_PENDING__");
    } else {
        snprintf(response, MAX_RSP_LEN, "Started %s PID %d", id, pid);
    }
}

static void handle_ps(char *response) {
    char buf[MAX_RSP_LEN];
    int off = 0;
    
    off += snprintf(buf + off, sizeof(buf) - off,
                    "%-16s %-8s %-14s %-12s %-10s %-10s\n",
                    "ID", "PID", "STATE", "START", "SOFT_MIB", "HARD_MIB");
    
    pthread_mutex_lock(&g_meta_lock);
    for (int i = 0; i < g_num_containers; i++) {
        Container *c = &g_containers[i];
        char ts[32];
        struct tm *tm_info = localtime(&c->start_time);
        strftime(ts, sizeof(ts), "%H:%M:%S", tm_info);
        
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8d %-14s %-12s %-10lu %-10lu\n",
                        c->id, c->pid, state_str(c->state), ts,
                        c->soft_mib, c->hard_mib);
        if (off >= (int)sizeof(buf) - 200) break;
    }
    pthread_mutex_unlock(&g_meta_lock);
    
    strncpy(response, buf, MAX_RSP_LEN - 1);
    response[MAX_RSP_LEN - 1] = '\0';
}

static void handle_logs(char *args, char *response) {
    char id[MAX_ID_LEN] = {0};
    sscanf(args, "%63s", id);
    
    char path[300];
    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, id);
    
    FILE *f = fopen(path, "r");
    if (!f) {
        snprintf(response, MAX_RSP_LEN, "No logs for '%s'", id);
        return;
    }
    
    size_t n = fread(response, 1, MAX_RSP_LEN - 1, f);
    response[n] = '\0';
    fclose(f);
    
    if (n == 0) {
        snprintf(response, MAX_RSP_LEN, "(empty)");
    }
}

static void handle_stop(char *args, char *response) {
    char id[MAX_ID_LEN] = {0};
    sscanf(args, "%63s", id);
    
    pthread_mutex_lock(&g_meta_lock);
    Container *found = NULL;
    for (int i = 0; i < g_num_containers; i++) {
        if (strcmp(g_containers[i].id, id) == 0 &&
            g_containers[i].state == STATE_RUNNING) {
            found = &g_containers[i];
            break;
        }
    }
    
    if (!found) {
        pthread_mutex_unlock(&g_meta_lock);
        snprintf(response, MAX_RSP_LEN, "Container '%s' not found", id);
        return;
    }
    
    found->stop_requested = 1;
    pid_t pid = found->pid;
    pthread_mutex_unlock(&g_meta_lock);
    
    kill(pid, SIGTERM);
    sleep(1);
    
    if (kill(pid, 0) == 0) {
        kill(pid, SIGKILL);
    }
    
    snprintf(response, MAX_RSP_LEN, "Stopped %s", id);
}

static void run_supervisor(char *rootfs) {
    (void)rootfs;
    
    mkdir(LOG_DIR, 0755);
    
    log_buf_init(&g_log_buf);
    pthread_create(&g_consumer_tid, NULL, consumer_thread_fn, NULL);

    pthread_t reaper_tid;
    pthread_create(&reaper_tid, NULL, reaper_thread_fn, NULL);
    pthread_detach(reaper_tid);
    
    g_monitor_fd = open("/dev/" DEVICE_NAME, O_RDWR);
    if (g_monitor_fd < 0) {
        fprintf(stderr, "Warning: kernel monitor not available\n");
    }
    
    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    
    sa.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    
    unlink(SOCKET_PATH);
    int srv_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);
    
    bind(srv_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(srv_fd, 16);
    
    printf("[engine] Supervisor started. Socket: %s\n", SOCKET_PATH);
    fflush(stdout);
    
    while (!g_shutdown) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(srv_fd, &fds);
        
        struct timeval tv = {1, 0};
        if (select(srv_fd + 1, &fds, NULL, NULL, &tv) <= 0) {
            continue;
        }
        
        int client = accept(srv_fd, NULL, NULL);
        if (client < 0) continue;
        
        char cmd_buf[MAX_CMD_LEN] = {0};
        read(client, cmd_buf, sizeof(cmd_buf) - 1);
        cmd_buf[strcspn(cmd_buf, "\n")] = '\0';
        
        char response[MAX_RSP_LEN] = {0};
        int keep_fd = 0;
        
        if (strncmp(cmd_buf, "start ", 6) == 0) {
            handle_start(cmd_buf + 6, response, -1);
        } else if (strncmp(cmd_buf, "run ", 4) == 0) {
            handle_start(cmd_buf + 4, response, client);
            if (strcmp(response, "__RUN_PENDING__") == 0) {
                keep_fd = 1;
            }
        } else if (strcmp(cmd_buf, "ps") == 0) {
            handle_ps(response);
        } else if (strncmp(cmd_buf, "logs ", 5) == 0) {
            handle_logs(cmd_buf + 5, response);
        } else if (strncmp(cmd_buf, "stop ", 5) == 0) {
            handle_stop(cmd_buf + 5, response);
        } else {
            snprintf(response, MAX_RSP_LEN, "Unknown command");
        }
        
        if (!keep_fd) {
            write(client, response, strlen(response));
            write(client, "\n", 1);
            close(client);
        }
    }
    
    printf("[engine] Shutting down...\n");
    
    pthread_mutex_lock(&g_meta_lock);
    for (int i = 0; i < g_num_containers; i++) {
        if (g_containers[i].state == STATE_RUNNING) {
            g_containers[i].stop_requested = 1;
            kill(g_containers[i].pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&g_meta_lock);
    
    sleep(1);
    
    pthread_mutex_lock(&g_log_buf.lock);
    g_log_buf.done = 1;
    pthread_cond_broadcast(&g_log_buf.not_empty);
    pthread_mutex_unlock(&g_log_buf.lock);
    
    pthread_join(g_consumer_tid, NULL);
    
    if (g_monitor_fd >= 0) close(g_monitor_fd);
    close(srv_fd);
    unlink(SOCKET_PATH);
    
    printf("[engine] Supervisor exited.\n");
}

static int cli_send(const char *cmd, int wait_long) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);
    
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor. Is it running?\n");
        close(fd);
        return 1;
    }
    
    write(fd, cmd, strlen(cmd));
    write(fd, "\n", 1);
    
    char buf[MAX_RSP_LEN * 4];
    ssize_t total = 0;

    if (wait_long) {
        ssize_t n;
        while ((n = read(fd, buf + total, sizeof(buf) - total - 1)) > 0)
            total += n;
    } else {
        total = read(fd, buf, sizeof(buf) - 1);
    }
    
    if (total > 0) {
        buf[total] = '\0';
        printf("%s", buf);
        if (buf[total-1] != '\n') printf("\n");
    }
    
    close(fd);
    return 0;
}

static void print_usage(void) {
    printf("Usage:\n");
    printf("  engine supervisor <base-rootfs>\n");
    printf("  engine start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n");
    printf("  engine run <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n");
    printf("  engine ps\n");
    printf("  engine logs <id>\n");
    printf("  engine stop <id>\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            print_usage();
            return 1;
        }
        run_supervisor(argv[2]);
        return 0;
    }
    
    char cmd[MAX_CMD_LEN] = {0};
    
    if (strcmp(argv[1], "ps") == 0) {
        return cli_send("ps", 0);
    }
    
    if (strcmp(argv[1], "logs") == 0) {
        if (argc < 3) return 1;
        snprintf(cmd, sizeof(cmd), "logs %s", argv[2]);
        return cli_send(cmd, 0);
    }
    
    if (strcmp(argv[1], "stop") == 0) {
        if (argc < 3) return 1;
        snprintf(cmd, sizeof(cmd), "stop %s", argv[2]);
        return cli_send(cmd, 0);
    }
    
    if (strcmp(argv[1], "start") == 0 || strcmp(argv[1], "run") == 0) {
        if (argc < 5) {
            print_usage();
            return 1;
        }
        
        char argstr[MAX_CMD_LEN] = {0};
        strncat(argstr, argv[2], sizeof(argstr) - 1);
        strncat(argstr, " ", sizeof(argstr) - 1);
        strncat(argstr, argv[3], sizeof(argstr) - 1);
        strncat(argstr, " ", sizeof(argstr) - 1);
        strncat(argstr, argv[4], sizeof(argstr) - 1);
        
        for (int i = 5; i < argc; i++) {
            strncat(argstr, " ", sizeof(argstr) - 1);
            strncat(argstr, argv[i], sizeof(argstr) - 1);
        }
        
        snprintf(cmd, sizeof(cmd), "%s %s", argv[1], argstr);
        return cli_send(cmd, strcmp(argv[1], "run") == 0);
    }
    
    print_usage();
    return 1;
}
