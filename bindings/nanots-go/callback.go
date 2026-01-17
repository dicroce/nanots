package nanots

/*
#include <stdlib.h>
#include <stdint.h>

typedef void (*nanots_read_callback_t)(const uint8_t* data,
                                       size_t size,
                                       uint8_t flags,
                                       int64_t timestamp,
                                       int64_t block_sequence,
                                       const char* metadata,
                                       void* user_data);
*/
import "C"
import (
	"unsafe"
)

//export goReadCallback
func goReadCallback(data *C.uint8_t, size C.size_t, flags C.uint8_t,
	timestamp C.int64_t, blockSequence C.int64_t,
	metadata *C.char, userData unsafe.Pointer) {

	// Get the callback ID from user_data
	id := uintptr(userData)

	callbackMu.Lock()
	callback, ok := callbackStore[id]
	callbackMu.Unlock()

	if !ok {
		return
	}

	// Create Go slice from C data
	var goData []byte
	if size > 0 {
		goData = C.GoBytes(unsafe.Pointer(data), C.int(size))
	}

	frame := Frame{
		Data:          goData,
		Timestamp:     int64(timestamp),
		BlockSequence: int64(blockSequence),
		Flags:         uint8(flags),
		Metadata:      C.GoString(metadata),
	}

	// Call the Go callback (ignore errors for now as C doesn't have a way to propagate them)
	_ = callback(frame)
}
