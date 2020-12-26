#ifndef _HASH_H
#define _HASH_H

// xxHash Implementation

#include <stdint.h>

uint64_t
hash(const void *input, uint64_t len, uint64_t seed);

#endif
