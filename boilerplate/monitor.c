/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Implements:
 *   - /dev/container_monitor char device
 *   - ioctl: MONITOR_REGISTER, MONITOR_UNREGISTER
 *   - Kernel linked list of tracked containers (mutex-protected)
 *   - Periodic timer checking RSS vs soft/hard limits
 *   - Soft limit: one-time printk warning
 *   - Hard limit: SIGKILL + remove entry
 *   - Clean list free on module unload
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include "monitor_ioctl.h"

#if LINUX_VERSION_CODE < KERNEL_VERSION(6, 15, 0)
#define timer_delete_sync(t)  del_timer_sync(t)
#define timer_delete(t)       del_timer(t)
#endif

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1 DONE: Linked-list node struct
 * ============================================================== */
struct monitored_container {
    char           container_id[64];
    pid_t          pid;
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            soft_warned;     /* 1 after first soft-limit warning */
    struct list_head list;
};

/* ==============================================================
 * TODO 2 DONE: Global list + mutex lock
 *
 * We use a mutex (not a spinlock) because:
 *   - The timer callback runs in softirq context but we use
 *     mod_timer so it runs in process context here.
 *   - get_rss_bytes() calls get_task_mm() which can sleep,
 *     so a spinlock would be inappropriate.
 * ============================================================== */
static LIST_HEAD(g_container_list);
static DEFINE_MUTEX(g_list_lock);

/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t             dev_num;
static struct cdev       c_dev;
static struct class     *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: Soft-limit warning helper
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: Hard-limit kill helper
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu - KILLED\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * TODO 3 DONE: Timer callback - periodic RSS monitoring
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    struct monitored_container *entry, *tmp;

    mutex_lock(&g_list_lock);

    /*
     * Use list_for_each_entry_safe so we can delete entries during
     * iteration without corrupting the list (safe variant uses a
     * temporary 'tmp' pointer to hold next before possible deletion).
     */
    list_for_each_entry_safe(entry, tmp, &g_container_list, list) {
        long rss = get_rss_bytes(entry->pid);

        /* Process no longer exists - remove stale entry */
        if (rss < 0) {
            printk(KERN_INFO
                   "[container_monitor] container=%s pid=%d exited, removing\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Hard limit check (takes priority) */
        if ((unsigned long)rss >= entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Soft limit check (warn once) */
        if (!entry->soft_warned &&
            (unsigned long)rss >= entry->soft_limit_bytes) {
            log_soft_limit_event(entry->container_id, entry->pid,
                                 entry->soft_limit_bytes, rss);
            entry->soft_warned = 1;
        }
    }

    mutex_unlock(&g_list_lock);

    /* Reschedule timer */
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    /* ============================================================
     * TODO 4 DONE: Add a monitored entry on MONITOR_REGISTER
     * ============================================================ */
    if (cmd == MONITOR_REGISTER) {
        struct monitored_container *entry;

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid,
               req.soft_limit_bytes, req.hard_limit_bytes);

        /* Basic validation */
        if (req.pid <= 0 || req.soft_limit_bytes == 0 || req.hard_limit_bytes == 0) {
            printk(KERN_WARNING "[container_monitor] Invalid registration params\n");
            return -EINVAL;
        }
        if (req.soft_limit_bytes >= req.hard_limit_bytes) {
            printk(KERN_WARNING
                   "[container_monitor] soft_limit must be < hard_limit\n");
            return -EINVAL;
        }

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        memset(entry, 0, sizeof(*entry));
        strncpy(entry->container_id, req.container_id,
                sizeof(entry->container_id) - 1);
        entry->pid              = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned      = 0;
        INIT_LIST_HEAD(&entry->list);

        mutex_lock(&g_list_lock);
        list_add_tail(&entry->list, &g_container_list);
        mutex_unlock(&g_list_lock);

        return 0;
    }

    /* ============================================================
     * TODO 5 DONE: Remove a monitored entry on MONITOR_UNREGISTER
     * ============================================================ */
    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    {
        struct monitored_container *entry, *tmp;
        int found = 0;

        mutex_lock(&g_list_lock);
        list_for_each_entry_safe(entry, tmp, &g_container_list, list) {
            if (entry->pid == req.pid &&
                strncmp(entry->container_id, req.container_id,
                        sizeof(entry->container_id)) == 0) {
                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }
        mutex_unlock(&g_list_lock);

        if (!found) {
            printk(KERN_INFO
                   "[container_monitor] Unregister: not found container=%s pid=%d\n",
                   req.container_id, req.pid);
            return -ENOENT;
        }
    }

    return 0;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n",
           DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    /* Stop timer before freeing list to avoid races */
    timer_delete_sync(&monitor_timer);

    /* ============================================================
     * TODO 6 DONE: Free all remaining monitored entries
     * ============================================================ */
    {
        struct monitored_container *entry, *tmp;

        mutex_lock(&g_list_lock);
        list_for_each_entry_safe(entry, tmp, &g_container_list, list) {
            printk(KERN_INFO
                   "[container_monitor] Freeing container=%s pid=%d on unload\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
        }
        mutex_unlock(&g_list_lock);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded cleanly.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
