#include "nanoarrow.h"

int arrow_array_append_bytes(struct ArrowArray* array, struct ArrowBufferView value) {
    return ArrowArrayAppendBytes(array, value);
}
