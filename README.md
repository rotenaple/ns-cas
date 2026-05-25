# NS Coordinated Allocation Server

A lightweight Go service that coordinates NationStates API quota across multiple appliances on your LAN. Clients acquire a ticket from CAS before calling NS, then report back the response headers, eliminating 429s.

---

## Quick Start

```bash
git clone https://github.com/rotenaple/ns-cas && cd ns-cas
docker compose build
docker compose up -d
curl http://192.168.1.50:8080/healthz
```

Open `http://192.168.1.50:8080/dashboard` for the live analytics dashboard.

---

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/acquire` | POST | Long-poll for a dispatch ticket (blocks up to 30 s) |
| `/report` | POST | Fire-and-forget telemetry after NS responds |
| `/report-and-acquire` | POST | Report the prior ticket and acquire a new one atomically |
| `/status` | GET | Live quota state |
| `/healthz` | GET | Liveness probe |
| `/api/logs` | GET | Recent request log (JSON, `?n=100`) |
| `/api/windows` | GET | Recent window detections (JSON) |
| `/api/anomalies` | GET | Recent anomalies (JSON) |
| `/api/stats` | GET | Aggregate statistics (JSON) |

### Acquire a ticket

```json
// POST /acquire {"appliance_id": "node-04", "priority_class": "P1_HIGH", "backlog": 3}
// → 200 {"action": "PROCEED", "token": "550e8400-..."}
// → 409 (ticket already active for this appliance)
// → 503 (queue full or timeout)
```

Save the `token` — you need it for `/report`.

`backlog` *(optional, default 0)* — the number of additional requests this appliance has waiting
behind the current one. CAS uses `1 + backlog` to weight demand for [class quota allocation](#class-based-quota-allocation), giving high-throughput appliances a proportionally larger slice of the next window.

### Report telemetry

```json
// POST /report → 204
{
  "token": "550e8400-...",
  "appliance_id": "node-04",
  "priority_class": "P1_HIGH",
  "backlog": 0,
  "queued_at": "2026-05-23T14:32:00.010Z",
  "acquired_at": "2026-05-23T14:32:00.012Z",
  "api_sent_at": "2026-05-23T14:32:00.015Z",
  "api_recv_at": "2026-05-23T14:32:00.615Z",
  "ns_remaining": 47,
  "ns_reset": 27,
  "ns_rate_limit": 50,
  "status_code": 200,
  "retry_after": 0
}
```

All timestamps are RFC 3339. `retry_after` is only non-zero when `status_code = 429`.

For `/report-and-acquire`, include `next_priority_class` (the class of the next request) and
optionally `backlog` (same semantics as `/acquire`).

```json
// POST /report-and-acquire → 200
{
  "token": "550e8400-...",
  "appliance_id": "node-04",
  "priority_class": "P1_HIGH",
  "next_priority_class": "P1_HIGH",
  "backlog": 0,
  "queued_at": "2026-05-23T14:32:00.010Z",
  "acquired_at": "2026-05-23T14:32:00.012Z",
  "api_sent_at": "2026-05-23T14:32:00.015Z",
  "api_recv_at": "2026-05-23T14:32:00.615Z",
  "ns_remaining": 47,
  "ns_reset": 27,
  "ns_rate_limit": 50,
  "status_code": 200,
  "retry_after": 0
}
```

---

## Priority Classes

The server expects each application to use **one** priority class based on the type of work:

| Class | Recommended use |
|-------|----------------|
| `P1_HIGH` | Latency-sensitive requests |
| `P2_MEDIUM` | Bulk requests |
| `P3_LOW` | Automated processes |

---

## Class-Based Quota Allocation

At each window boundary CAS allocates the full `BucketLimit` (e.g. 50) across the three
priority classes in proportion to demand observed in the prior window, subject to a hard
per-class ceiling:

| Class | Weight | Max fraction |
|-------|--------|-------------|
| `P1_HIGH` | 4 | 50 % |
| `P2_MEDIUM` | 2 | 70 % |
| `P3_LOW` | 1 | 100 % |

**Example** — only P1 and P2 have demand (total weight = 6):

- P1 raw = round(50 × 4/6) = 33 → capped at floor(50 × 0.5) = 25 → **25 slots**
- P2 raw = round(50 × 2/6) = 17 → within 70 % cap → **17 slots**
- Shared spill pool: 50 − 25 − 17 = **8 slots** (any class may use these)

Classes with no demand are excluded from the proportional split so their weight doesn't
dilute active classes. Unused class quota from one class does not carry over; unused shared
quota is discarded at the next reset.

The `backlog` field shapes this allocation: CAS counts `1 + backlog` units of demand per
acquire call, so an appliance with 9 requests waiting signals 10 units, not 1.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CAS_PORT` | `8080` | HTTP listen port |
| `CAS_DB_PATH` | `/data/cas.db` | SQLite database path |
| `CAS_AGING_WEIGHT` | `1.0` | Starvation prevention: rate at which low-priority tickets age into higher priority |
| `CAS_MAX_QUEUE` | `100` | Max long-poll connections before returning 503 |

---

## Features

- **Zero-429 enforcement** — never dispatches when `LocalRemaining - InFlight ≤ 0`
- **Class-based quota allocation** — at each window boundary, slots are divided across `P1_HIGH / P2_MEDIUM / P3_LOW` proportionally by demand with configurable hard ceilings; a shared spill pool absorbs unused budget
- **Priority scheduling** — `P1_HIGH` / `P2_MEDIUM` / `P3_LOW` with aging-based starvation prevention
- **Cold-start safety** — boots with `LocalRemaining = 1` until first telemetry calibrates state
- **30-second window tracking** — detects NS window boundaries via the 49-trigger with fallback on `RateLimit-Reset`
- **Hard lockdown on 429** — blocks all dispatch; resumes with half-bucket after `Retry-After`
- **Stale ticket reaper** — expired tickets (>10 s) reclaimed so crashed appliances never leak in-flight slots
- **Legacy interference measurement** — quantifies quota consumed by uncoordinated scripts between dispatch and response
- **Live dashboard** — auto-refreshing dark UI at `/dashboard`
- **Graceful shutdown** — `SIGTERM` drains the queue and closes SQLite cleanly

---

## Example Client Implementation (Python)

Drop this into your appliance script. Pattern: acquire → call NS → report.

```python

```python
import time
import threading
import logging
from datetime import datetime, timezone
from typing import Optional

import requests


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class CASClient:
    """Thread-safe CAS client. One instance per appliance."""

    def __init__(self, cas_url: str, appliance_id: str) -> None:
        self.cas_url = cas_url.rstrip("/")
        self.appliance_id = appliance_id
        self.logger = logging.getLogger(__name__)

    def acquire(self, priority: str = "P2_MEDIUM") -> Optional[str]:
        """
        Request a dispatch ticket. Long-polls up to 35 s.

        Returns a token on success, or None on fallback (always sleeps 1.5 s).
        """
        try:
            r = requests.post(
                f"{self.cas_url}/acquire",
                json={"appliance_id": self.appliance_id, "priority_class": priority},
                timeout=35,
            )
            if r.status_code == 409:
                self.logger.warning("CAS 409: appliance already has an active ticket")
                time.sleep(1.5)
                return None
            if r.status_code == 503:
                self.logger.warning("CAS 503: queue full or timeout")
                time.sleep(1.5)
                return None
            if r.ok:
                return r.json().get("token")
            self.logger.warning("CAS unexpected status %d", r.status_code)
            time.sleep(1.5)
            return None
        except requests.RequestException:
            self.logger.warning("CAS unreachable")
            time.sleep(1.5)
            return None

    def report(
        self,
        token: str,
        priority: str,
        queued_at: float,
        acquired_at: float,
        api_sent_at: float,
        api_recv_at: float,
        ns_remaining: int,
        ns_reset: int,
        ns_rate_limit: int,
        status_code: int,
        retry_after: int = 0,
    ) -> None:
        """Fire-and-forget telemetry to /report (daemon thread)."""
        payload = {
            "token": token,
            "appliance_id": self.appliance_id,
            "priority_class": priority,
            "queued_at": _iso(queued_at),
            "acquired_at": _iso(acquired_at),
            "api_sent_at": _iso(api_sent_at),
            "api_recv_at": _iso(api_recv_at),
            "ns_remaining": ns_remaining,
            "ns_reset": ns_reset,
            "ns_rate_limit": ns_rate_limit,
            "status_code": status_code,
            "retry_after": retry_after,
        }

        def _do() -> None:
            try:
                requests.post(f"{self.cas_url}/report", json=payload, timeout=2)
            except Exception:
                pass

        threading.Thread(target=_do, daemon=True).start()
```