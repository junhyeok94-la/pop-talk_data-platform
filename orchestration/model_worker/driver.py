"""격리 subprocess 진입점. 임의 코드/URL을 받지 않고 로컬 가중치만 사용한다."""

import json
import os
import sys
import time
from pathlib import Path


def main():
    folder = Path(sys.argv[1])
    spec = json.loads((folder / "request.json").read_text())
    if spec["kind"] == "diagnostic":
        from worker import resources
        from cuda_probe import probe

        before = resources()
        cuda_result = probe()
        # 외부 프로세스의 생존/취소/deferral 검증을 위한 짧은 진단 구간.
        time.sleep(12)
        result = {
            "kind": "diagnostic",
            "gpu_observed": bool(before["gpus"]),
            "cuda_probe": cuda_result,
            "resources": before,
            "worker_pid": os.getpid(),
            "training_performed": False,
        }
    else:
        from train import train

        result = train(spec, folder)
    (folder / "result.json").write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    main()
