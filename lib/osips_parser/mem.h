#ifndef OSIPS_MEM_STUBS_H
#define OSIPS_MEM_STUBS_H

#include <stdlib.h>

#define pkg_malloc malloc
#define pkg_free free
#define pkg_realloc realloc
#define pkg_malloc_func malloc
#define pkg_free_func free
#define pkg_realloc_func realloc
#define shm_malloc malloc
#define shm_free free
#define shm_realloc realloc
#define shm_malloc_func malloc
#define shm_free_func free
#define shm_realloc_func realloc

typedef void *(*osips_malloc_f) (unsigned long size);
typedef void *(*osips_realloc_f) (void *ptr, unsigned long size);
typedef void (*osips_free_f) (void *ptr);

#define func_malloc(_func, _size) (_func)(_size)
#define func_realloc(_func, _ptr, _size) (_func)(_ptr, _size)
#define func_free(_func, _ptr) (_func)(_ptr)

#endif // OSIPS_MEM_STUBS_H
