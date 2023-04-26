/* SPDX-License-Identifier: MIT */
// https://syzkaller.appspot.com/bug?id=5c9918d20f771265ad0ffae3c8f3859d24850692
// autogenerated by syzkaller (https://github.com/google/syzkaller)

#include <endian.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"
#include "../src/syscall.h"

#define SIZEOF_IO_URING_SQE 64
#define SIZEOF_IO_URING_CQE 16
#define SQ_HEAD_OFFSET 0
#define SQ_TAIL_OFFSET 64
#define SQ_RING_MASK_OFFSET 256
#define SQ_RING_ENTRIES_OFFSET 264
#define SQ_FLAGS_OFFSET 276
#define SQ_DROPPED_OFFSET 272
#define CQ_HEAD_OFFSET 128
#define CQ_TAIL_OFFSET 192
#define CQ_RING_MASK_OFFSET 260
#define CQ_RING_ENTRIES_OFFSET 268
#define CQ_RING_OVERFLOW_OFFSET 284
#define CQ_FLAGS_OFFSET 280
#define CQ_CQES_OFFSET 320

static long syz_io_uring_setup(volatile long a0, volatile long a1,
                               volatile long a2, volatile long a3,
                               volatile long a4, volatile long a5)
{
  uint32_t entries = (uint32_t)a0;
  struct io_uring_params* setup_params = (struct io_uring_params*)a1;
  void* vma1 = (void*)a2;
  void* vma2 = (void*)a3;
  void** ring_ptr_out = (void**)a4;
  void** sqes_ptr_out = (void**)a5;
  uint32_t fd_io_uring = __sys_io_uring_setup(entries, setup_params);
  uint32_t sq_ring_sz =
      setup_params->sq_off.array + setup_params->sq_entries * sizeof(uint32_t);
  uint32_t cq_ring_sz = setup_params->cq_off.cqes +
                        setup_params->cq_entries * SIZEOF_IO_URING_CQE;
  uint32_t ring_sz = sq_ring_sz > cq_ring_sz ? sq_ring_sz : cq_ring_sz;
  *ring_ptr_out = mmap(vma1, ring_sz, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_POPULATE | MAP_FIXED, fd_io_uring,
                       IORING_OFF_SQ_RING);
  if (*ring_ptr_out == MAP_FAILED)
    exit(0);
  uint32_t sqes_sz = setup_params->sq_entries * SIZEOF_IO_URING_SQE;
  *sqes_ptr_out =
      mmap(vma2, sqes_sz, PROT_READ | PROT_WRITE,
           MAP_SHARED | MAP_POPULATE | MAP_FIXED, fd_io_uring, IORING_OFF_SQES);
  if (*sqes_ptr_out == MAP_FAILED)
    exit(0);
  return fd_io_uring;
}

static long syz_io_uring_submit(volatile long a0, volatile long a1,
                                volatile long a2, volatile long a3)
{
  char* ring_ptr = (char*)a0;
  char* sqes_ptr = (char*)a1;
  char* sqe = (char*)a2;
  uint32_t sqes_index = (uint32_t)a3;
  uint32_t sq_ring_entries = *(uint32_t*)(ring_ptr + SQ_RING_ENTRIES_OFFSET);
  uint32_t cq_ring_entries = *(uint32_t*)(ring_ptr + CQ_RING_ENTRIES_OFFSET);
  uint32_t sq_array_off =
      (CQ_CQES_OFFSET + cq_ring_entries * SIZEOF_IO_URING_CQE + 63) & ~63;
  if (sq_ring_entries)
    sqes_index %= sq_ring_entries;
  char* sqe_dest = sqes_ptr + sqes_index * SIZEOF_IO_URING_SQE;
  memcpy(sqe_dest, sqe, SIZEOF_IO_URING_SQE);
  uint32_t sq_ring_mask = *(uint32_t*)(ring_ptr + SQ_RING_MASK_OFFSET);
  uint32_t* sq_tail_ptr = (uint32_t*)(ring_ptr + SQ_TAIL_OFFSET);
  uint32_t sq_tail = *sq_tail_ptr & sq_ring_mask;
  uint32_t sq_tail_next = *sq_tail_ptr + 1;
  uint32_t* sq_array = (uint32_t*)(ring_ptr + sq_array_off);
  *(sq_array + sq_tail) = sqes_index;
  __atomic_store_n(sq_tail_ptr, sq_tail_next, __ATOMIC_RELEASE);
  return 0;
}

static long syz_open_dev(volatile long a0, volatile long a1, volatile long a2)
{
  if (a0 == 0xc || a0 == 0xb) {
    char buf[128];
    sprintf(buf, "/dev/%s/%d:%d", a0 == 0xc ? "char" : "block", (uint8_t)a1,
            (uint8_t)a2);
    return open(buf, O_RDWR, 0);
  } else {
    char buf[1024];
    char* hash;
    strncpy(buf, (char*)a0, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = 0;
    while ((hash = strchr(buf, '#'))) {
      *hash = '0' + (char)(a1 % 10);
      a1 /= 10;
    }
    return open(buf, a2, 0);
  }
}

uint64_t r[4] = {0xffffffffffffffff, 0x0, 0x0, 0xffffffffffffffff};

int main(int argc, char *argv[])
{
  void *mmap_ret;
#if !defined(__i386) && !defined(__x86_64__)
  return T_EXIT_SKIP;
#endif

  if (argc > 1)
    return T_EXIT_SKIP;

  mmap_ret = mmap((void *)0x20000000ul, 0x1000000ul, 7ul, 0x32ul, -1, 0ul);
  if (mmap_ret == MAP_FAILED)
    return T_EXIT_SKIP;
  mmap_ret = mmap((void *)0x21000000ul, 0x1000ul, 0ul, 0x32ul, -1, 0ul);
  if (mmap_ret == MAP_FAILED)
    return T_EXIT_SKIP;
  intptr_t res = 0;
  *(uint32_t*)0x20000484 = 0;
  *(uint32_t*)0x20000488 = 0;
  *(uint32_t*)0x2000048c = 0;
  *(uint32_t*)0x20000490 = 0;
  *(uint32_t*)0x20000498 = -1;
  *(uint32_t*)0x2000049c = 0;
  *(uint32_t*)0x200004a0 = 0;
  *(uint32_t*)0x200004a4 = 0;
  res = -1;
  res = syz_io_uring_setup(0x6ad4, 0x20000480, 0x20ee7000, 0x20ffb000,
                           0x20000180, 0x20000040);
  if (res != -1) {
    r[0] = res;
    r[1] = *(uint64_t*)0x20000180;
    r[2] = *(uint64_t*)0x20000040;
  }
  res = -1;
  res = syz_open_dev(0xc, 4, 0x15);
  if (res != -1)
    r[3] = res;
  *(uint8_t*)0x20000000 = 6;
  *(uint8_t*)0x20000001 = 0;
  *(uint16_t*)0x20000002 = 0;
  *(uint32_t*)0x20000004 = r[3];
  *(uint64_t*)0x20000008 = 0;
  *(uint64_t*)0x20000010 = 0;
  *(uint32_t*)0x20000018 = 0;
  *(uint16_t*)0x2000001c = 0;
  *(uint16_t*)0x2000001e = 0;
  *(uint64_t*)0x20000020 = 0;
  *(uint16_t*)0x20000028 = 0;
  *(uint16_t*)0x2000002a = 0;
  *(uint8_t*)0x2000002c = 0;
  *(uint8_t*)0x2000002d = 0;
  *(uint8_t*)0x2000002e = 0;
  *(uint8_t*)0x2000002f = 0;
  *(uint8_t*)0x20000030 = 0;
  *(uint8_t*)0x20000031 = 0;
  *(uint8_t*)0x20000032 = 0;
  *(uint8_t*)0x20000033 = 0;
  *(uint8_t*)0x20000034 = 0;
  *(uint8_t*)0x20000035 = 0;
  *(uint8_t*)0x20000036 = 0;
  *(uint8_t*)0x20000037 = 0;
  *(uint8_t*)0x20000038 = 0;
  *(uint8_t*)0x20000039 = 0;
  *(uint8_t*)0x2000003a = 0;
  *(uint8_t*)0x2000003b = 0;
  *(uint8_t*)0x2000003c = 0;
  *(uint8_t*)0x2000003d = 0;
  *(uint8_t*)0x2000003e = 0;
  *(uint8_t*)0x2000003f = 0;
  syz_io_uring_submit(r[1], r[2], 0x20000000, 0);
  __sys_io_uring_enter(r[0], 0x20450c, 0, 0ul, 0ul);
  *(uint32_t*)0x20000080 = 0x7ff;
  *(uint32_t*)0x20000084 = 0x8b7;
  *(uint32_t*)0x20000088 = 3;
  *(uint32_t*)0x2000008c = 0x101;
  *(uint8_t*)0x20000090 = 9;
  memcpy((void*)0x20000091, "\xaf\x09\x01\xbc\xf9\xc6\xe4\x92\x86\x51\x7d\x7f"
                            "\xbd\x43\x7d\x16\x69\x3e\x05",
         19);
  ioctl(r[3], 0x5404, 0x20000080ul);
  return T_EXIT_PASS;
}
