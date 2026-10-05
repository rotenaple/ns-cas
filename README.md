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
  "ns_policy": "50;w=30",
  "status_code": 200,
  "retry_after": 0
}
```

All timestamps are RFC 3339. `retry_after` is only non-zero when `status_code = 429`.
`ns_policy` is the `ratelimit-policy` header verbatim; send it, because it is the only
header that says how long an allocation lasts.

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
| `CAS_BUCKET_LIMIT` | `50` | Fallback burst allowance, used until `ratelimit-limit` arrives |
| `CAS_POLICY_WINDOW_SEC` | `30` | Fallback for the `w=` component of `ratelimit-policy` |
| `CAS_SUSTAINED_LIMIT` | `300` | Initial dispatches allowed per sustained window |
| `CAS_SUSTAINED_WINDOW_SEC` | `900` | Initial sustained window length; superseded by the `Retry-After` NS reports |
| `CAS_SUSTAINED_MIN_LIMIT` | `25` | Floor the sustained allowance is cut down to by penalties |
| `CAS_SUSTAINED_MAX_LIMIT` | `1200` | Ceiling the sustained allowance climbs back to |

---

## Rate limiting: what CAS runs on

CAS does not model NationStates' quota. It runs on the headers NS returns, in
both normal operation and penalty:

| Header | Use |
|--------|-----|
| `ratelimit-limit` | The allowance (`BucketLimit`) |
| `ratelimit-policy` | `50;w=30` — the only header that states how long an allocation lasts. Its `w=` is how long a reported remaining stays trustworthy |
| `ratelimit-remaining` | The budget actually left, which is what gates dispatch |
| `ratelimit-reset` | Countdown to the next refill. **Not** a window length |
| `Retry-After` (on 429) | Penalty duration, which becomes the sustained window |

The env vars above are cold-start and fallback only. `ratelimit-reset` is
deliberately not used as a window length: doing so grants a fresh allowance
several times faster than NS resets its own, which is what produced the
sustained 429s CAS is meant to prevent.

### Two budgets

**Burst** — `ratelimit-remaining` is authoritative. CAS dispatches only while
`remaining − in_flight > 0`. Once a reported figure is older than the policy
window it is discarded and the bucket is assumed refilled, because a stale `0`
would otherwise wedge CAS: the value only clears when a report arrives, and if
the budget is what stopped dispatch then no report is coming.

**Sustained** — NationStates also enforces a longer-run limit whose penalty
duration it reports per occurrence. CAS caps dispatches across that window, and
the window length is learned from `Retry-After` rather than assumed, since the
duration varies.

The sustained allowance adapts, because the limit NS enforces is not published:

- a 429 **halves** it — direct evidence the rate was too high — clamped to `CAS_SUSTAINED_MIN_LIMIT`
- a sustained window that completes without a 429 **adds one** back, up to `CAS_SUSTAINED_MAX_LIMIT`
- a window containing a penalty earns nothing back

Every change is logged with the old and new values, and `/status` reports the
live allowance against the configured baseline.

A 429 with no `Retry-After` keeps the window already learned rather than
collapsing the penalty to zero. Collapsing it released dispatch immediately and
walked straight back into another 429.

---

## Features

- **Zero-429 enforcement** — never dispatches when `ratelimit-remaining − in_flight ≤ 0`
- **Header-driven quota** — allowance, window length and budget all come from `ratelimit-limit` / `ratelimit-policy` / `ratelimit-remaining`, never from a local model
- **Sustained budget** — caps dispatches across the penalty window, whose length is learned from the `Retry-After` NS reports; the allowance halves on a 429 and recovers one request per clean window
- **Class-based quota allocation** — at each window boundary, slots are divided across `P1_HIGH / P2_MEDIUM / P3_LOW` proportionally by demand with configurable hard ceilings; a shared spill pool absorbs unused budget
- **Priority scheduling** — `P1_HIGH` / `P2_MEDIUM` / `P3_LOW` with aging-based starvation prevention
- **Cold-start safety** — boots with `LocalRemaining = 1` until first telemetry calibrates state
- **Stale-figure recovery** — a reported remaining older than the policy window is discarded rather than allowed to wedge dispatch
- **Hard lockdown on 429** — blocks all dispatch for the reported penalty duration; resumes at full allowance afterwards
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