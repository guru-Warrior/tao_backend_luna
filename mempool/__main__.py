"""The mempool monitor is served by FastAPI (`uvicorn mempool_server:app`); no standalone CLI."""

import sys

if __name__ == "__main__":
    sys.stderr.write(
        "Use: python -m uvicorn mempool_server:app --host 0.0.0.0 --port 8001\n"
    )
    sys.exit(2)
