"""PyInstaller entry point — wraps uvicorn programmatically so the frozen executable works."""
import multiprocessing
import sys

import uvicorn


def main() -> None:
    if "--version" in sys.argv:
        print("queryastra 0.1.0")
        return

    host = "127.0.0.1"
    port = 8000
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--host" and i < len(sys.argv):
            host = sys.argv[i + 1]
        if arg == "--port" and i < len(sys.argv):
            port = int(sys.argv[i + 1])

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
