"""启动开发服务器：python3 tools/run_server.py [host] [port]

数据库连接默认 sqlite:///./transfer.sqlite3，可用环境变量覆盖：
    TRANSFER_DATABASE_URL=postgresql+psycopg://user:pass@host/db
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


if __name__ == "__main__":
    import uvicorn

    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    uvicorn.run("transfer_backend.app:app", host=host, port=port, reload=False)
