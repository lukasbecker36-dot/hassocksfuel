"""One-shot poller for use with Windows Task Scheduler / cron."""
from __future__ import annotations

import logging
import sys

from src import settings
from src.api_client import FuelFinderClient
from src.db import init_db
from src.poller import poll_once
from src.turso_db import TursoDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    conn = init_db()
    client = FuelFinderClient()

    turso: TursoDB | None = None
    if settings.turso_url and settings.turso_token:
        turso = TursoDB()
        turso.init_schema()

    try:
        poll_once(client, conn, turso)
    except Exception:
        log.exception("Poll failed")
        sys.exit(1)
    finally:
        client.close()
        conn.close()
        if turso:
            turso.close()


if __name__ == "__main__":
    main()
