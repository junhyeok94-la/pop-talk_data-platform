"""작은 CUDA 메모리 할당/초기화로 컨테이너의 실제 GPU 접근을 확인한다."""

import ctypes
import time


def probe():
    cuda = ctypes.CDLL("libcuda.so.1")

    def check(status):
        if status:
            raise RuntimeError(f"CUDA driver error: {status}")

    check(cuda.cuInit(0))
    device = ctypes.c_int()
    check(cuda.cuDeviceGet(ctypes.byref(device), 0))
    ctx = ctypes.c_void_p()
    check(cuda.cuDevicePrimaryCtxRetain(ctypes.byref(ctx), device))
    try:
        check(cuda.cuCtxSetCurrent(ctx))
        ptr = ctypes.c_uint64()
        size = ctypes.c_size_t(64 * 1024 * 1024)
        started = time.monotonic()
        check(cuda.cuMemAlloc_v2(ctypes.byref(ptr), size))
        try:
            check(cuda.cuMemsetD8_v2(ptr, ctypes.c_ubyte(0), size))
            check(cuda.cuCtxSynchronize())
        finally:
            check(cuda.cuMemFree_v2(ptr))
        return {"allocated_mib": 64, "seconds": round(time.monotonic() - started, 4)}
    finally:
        check(cuda.cuDevicePrimaryCtxRelease_v2(device))
