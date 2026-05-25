// Package dashboard serves read-only analytics pages and JSON API endpoints.
package dashboard

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/rotenaple/ns-cas/internal/db"
	"github.com/rotenaple/ns-cas/internal/handlers"
	"github.com/rotenaple/ns-cas/internal/state"
)

// RegisterRoutes wires dashboard routes.
func RegisterRoutes(mux *http.ServeMux, database *db.DB, eng *handlers.Engine, st *state.State) {
	h := &dashHandler{db: database, eng: eng, state: st}
	mux.HandleFunc("/dashboard", h.serveDashboard)
	mux.HandleFunc("/dashboard/", h.serveDashboard)
	mux.HandleFunc("/api/logs", h.apiLogs)
	mux.HandleFunc("/api/windows", h.apiWindows)
	mux.HandleFunc("/api/anomalies", h.apiAnomalies)
	mux.HandleFunc("/api/stats", h.apiStats)
	mux.HandleFunc("/api/events", h.serveEvents)
}

type dashHandler struct {
	db    *db.DB
	eng   *handlers.Engine
	state *state.State
}

// liveStateJSON is the live state shared by HTML template and SSE events.
type liveStateJSON struct {
	LocalRemaining  int    `json:"remaining"`
	InFlight        int    `json:"inFlight"`
	Available       int    `json:"available"`
	QueueDepth      int    `json:"queueDepth"`
	LockdownActive  bool   `json:"lockdownActive"`
	LockdownDisplay string `json:"lockdownDisplay"`
	WindowDisplay   string `json:"windowDisplay"`
	WindowAccuracy  string `json:"windowAccuracy"`
	WindowEndUnix   int64  `json:"windowEndUnix"`
	P1Remaining     int    `json:"p1Remaining"`
	P2Remaining     int    `json:"p2Remaining"`
	P3Remaining     int    `json:"p3Remaining"`
	SharedRemaining int    `json:"sharedRemaining"`
}

func (h *dashHandler) buildLiveState() liveStateJSON {
	h.state.Lock()
	defer h.state.Unlock()
	avail := h.state.LocalRemaining - h.state.InFlight
	if avail < 0 {
		avail = 0
	}

	windowDisplay := ""
	windowAccuracy := ""
	windowEndUnix := int64(0)
	if !h.state.Calibrated {
		windowDisplay = "—"
	} else if h.state.WindowActive {
		windowEndUnix = h.state.WindowEndTime.UnixMilli()
		remaining := time.Until(h.state.WindowEndTime)
		if remaining < 0 {
			remaining = 0
		}
		windowDisplay = fmt.Sprintf("%ds", int(remaining.Seconds()))
		if h.state.WindowDetectedVia == "49_trigger" {
			windowAccuracy = "high"
		}
	} else {
		windowDisplay = "N/A"
	}

	lockdownDisplay := "None"
	if h.state.LockdownActive {
		lockdownDisplay = "429 — until " + h.state.LockdownUntil.UTC().Format("15:04:05 UTC")
	}

	return liveStateJSON{
		LocalRemaining:  h.state.LocalRemaining,
		InFlight:        h.state.InFlight,
		Available:       avail,
		QueueDepth:      h.eng.Queue.Len(),
		LockdownActive:  h.state.LockdownActive,
		LockdownDisplay: lockdownDisplay,
		WindowDisplay:   windowDisplay,
		WindowAccuracy:  windowAccuracy,
		WindowEndUnix:   windowEndUnix,
		P1Remaining:     h.state.ClassRemaining[state.PriorityHigh],
		P2Remaining:     h.state.ClassRemaining[state.PriorityMedium],
		P3Remaining:     h.state.ClassRemaining[state.PriorityLow],
		SharedRemaining: h.state.SharedRemaining,
	}
}

// ---- HTML dashboard ----

var dashTmpl = template.Must(template.New("dash").Funcs(template.FuncMap{
	"derefInt": func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	},
	"derefFloat": func(p *float64) float64 {
		if p == nil {
			return 0
		}
		return *p
	},
	"derefStr": func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	},
	"shortTime": func(s string) template.HTML {
		if len(s) >= 19 {
			return template.HTML(s[:10] + `<wbr>` + s[10:])
		}
		return template.HTML(s)
	},
}).Parse(dashboardHTML))

func (h *dashHandler) serveDashboard(w http.ResponseWriter, r *http.Request) {
	applianceFilter := r.URL.Query().Get("appliance")
	now := time.Now()
	p1m, _ := h.db.QueryStatsSince(now.Add(-1*time.Minute), applianceFilter)
	p1h, _ := h.db.QueryStatsSince(now.Add(-1*time.Hour), applianceFilter)
	p1d, _ := h.db.QueryStatsSince(now.Add(-24*time.Hour), applianceFilter)
	p30d, _ := h.db.QueryStatsSince(now.Add(-720*time.Hour), applianceFilter)
	logs, _ := h.db.QueryRecentLogs(50)
	windows, _ := h.db.QueryRecentWindows(20)
	anomalies, _ := h.db.QueryRecentAnomalies(20)
	appliances, _ := h.db.QueryDistinctAppliances()

	liveState := h.buildLiveState()

	type period struct {
		Label          string
		Secs           float64
		Stats          db.PeriodStats
		ThroughputExcl float64 // requests /30s excluding external consumption
		ThroughputIncl float64 // (requests+legacy) /30s including external consumption
	}
	periods := []period{
		{"1m", 60, p1m, 0, 0},
		{"1h", 3600, p1h, 0, 0},
		{"1d", 86400, p1d, 0, 0},
		{"30d", 2592000, p30d, 0, 0},
	}
	for i := range periods {
		windows := periods[i].Secs / 30
		if windows > 0 {
			periods[i].ThroughputExcl = float64(periods[i].Stats.TotalRequests) / windows
			periods[i].ThroughputIncl = float64(periods[i].Stats.TotalRequests+periods[i].Stats.TotalLegacyUsage) / windows
		}
	}

	data := struct {
		GeneratedAt     string
		Live            interface{}
		Periods         []period
		Logs            []db.RecentLog
		Windows         []db.WindowRow
		Anomalies       []db.AnomalyRow
		Appliances      []string
		ApplianceFilter string
	}{
		GeneratedAt:     time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		Live:            liveState,
		Periods:         periods,
		Logs:            logs,
		Windows:         windows,
		Anomalies:       anomalies,
		Appliances:      appliances,
		ApplianceFilter: applianceFilter,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashTmpl.Execute(w, data); err != nil {
		log.Printf("[dashboard] template error: %v", err)
	}
}

// ---- JSON API ----

func (h *dashHandler) apiLogs(w http.ResponseWriter, r *http.Request) {
	n := queryIntDefault(r, "n", 100)
	rows, err := h.db.QueryRecentLogs(n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (h *dashHandler) apiWindows(w http.ResponseWriter, r *http.Request) {
	n := queryIntDefault(r, "n", 50)
	rows, err := h.db.QueryRecentWindows(n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (h *dashHandler) apiAnomalies(w http.ResponseWriter, r *http.Request) {
	n := queryIntDefault(r, "n", 50)
	rows, err := h.db.QueryRecentAnomalies(n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (h *dashHandler) apiStats(w http.ResponseWriter, _ *http.Request) {
	stats, err := h.db.QueryStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, stats)
}

func (h *dashHandler) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	id, ch := h.eng.Notifier.Subscribe()
	defer h.eng.Notifier.Unsubscribe(id)

	data, _ := json.Marshal(h.buildLiveState())
	fmt.Fprintf(w, "event: live\ndata: %s\n\n", data)
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ch:
			data, _ = json.Marshal(h.buildLiveState())
			fmt.Fprintf(w, "event: live\ndata: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[dashboard] json encode: %v", err)
	}
}

func queryIntDefault(r *http.Request, key string, def int) int {
	if s := r.URL.Query().Get(key); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// ---- Embedded HTML template ----

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CAS Dashboard (live)</title>
<style>
  :root{--bg:#0f1117;--surface:#1a1d27;--border:#2d3148;--accent:#6c8cff;--green:#4caf6e;--red:#e05c5c;--yellow:#e0b84c;--text:#d4d8f0;--muted:#7880a0}
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5}
  header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:16px}
  header h1{font-size:1.1rem;font-weight:600;color:var(--accent)}
  .ts{font-size:.75rem;color:var(--muted);margin-left:auto}
  main{padding:20px 24px;display:grid;gap:20px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px}
  .card h2{font-size:.8rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
  .grid-4{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}
  .metric{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px}
  .metric .val{font-size:1.8rem;font-weight:700;color:var(--accent)}
  .metric .lbl{font-size:.72rem;color:var(--muted);margin-top:2px}
  .badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.72rem;font-weight:600}
  .ok{background:#1a3a26;color:var(--green)}.warn{background:#3a2e12;color:var(--yellow)}.err{background:#3a1212;color:var(--red)}
  table{width:100%;border-collapse:collapse;font-size:.78rem}
  th{text-align:left;padding:6px 8px;color:var(--muted);border-bottom:1px solid var(--border);white-space:nowrap}
  td{padding:5px 8px;border-bottom:1px solid #1e2135;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}
  tr:hover td{background:#20243a}
  .stat-label{color:var(--muted);white-space:nowrap;padding:4px 12px 4px 4px;border-bottom:1px solid #1e2135;font-size:.78rem}
  .stat-val{padding:4px 12px;border-bottom:1px solid #1e2135;font-size:.82rem;text-align:center}
  .tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.68rem;font-weight:600}
  .filter-row{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}
  .filter-input,.filter-select{background:var(--bg);border:1px solid var(--border);border-radius:4px;color:var(--text);padding:4px 8px;font-size:.78rem;width:180px;outline:none}
  .filter-input:focus,.filter-select:focus{border-color:var(--accent)}
  .p1{background:#1a2a50;color:#6ab0ff}.p2{background:#1a3520;color:#6adf8a}.p3{background:#2a2a1a;color:#dfcf6a}
  .det-primary{background:#1a3040;color:#6acfff}.det-fallback{background:#2a1a40;color:#bf8fff}
  .status-ok{color:var(--green)}.status-err{color:var(--red)}.status-warn{color:var(--yellow)}
  .live-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}
  .live-item{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px}
  .live-item .v{font-size:1.4rem;font-weight:700}
  .live-item .k{font-size:.7rem;color:var(--muted);margin-top:2px}
  .acc-high{background:#1a3a40;color:#4adfcf;font-size:.65rem}
  .acc-low{background:#3a2e1a;color:#dfcf6a;font-size:.65rem}
  a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
@media(max-width:768px){main{padding:12px;gap:12px}.card{padding:12px}.card h2{margin-bottom:8px}
header{padding:10px 12px;gap:10px;flex-wrap:wrap}header h1{font-size:1rem}.ts{font-size:.7rem}
.live-grid{grid-template-columns:repeat(3,1fr)}.live-item{padding:8px}.live-item .v{font-size:1.2rem}
.grid-4{grid-template-columns:repeat(2,1fr)}.metric{padding:8px}.metric .val{font-size:1.4rem}
.filter-input{width:140px;font-size:.72rem}}
@media(max-width:480px){body{font-size:13px}main{padding:8px;gap:8px}.card{padding:8px;border-radius:6px}
header{padding:8px 10px;gap:6px}header h1{font-size:.9rem}
.live-grid{grid-template-columns:repeat(2,1fr)}.live-item .v{font-size:1rem}.live-item{padding:6px}
.grid-4{grid-template-columns:repeat(2,1fr)}.metric .val{font-size:1.2rem}.metric .lbl{font-size:.65rem}.metric{padding:6px}
th{padding:2px 3px;font-size:.62rem}td{padding:2px 3px;font-size:.62rem;white-space:normal;max-width:85px;overflow:hidden}
.filter-row{flex-wrap:wrap}.filter-input{width:100%;font-size:.72rem}}
.filter-row{flex-wrap:wrap}.filter-input{width:100%;font-size:.72rem}}
</style>
</head>
<body>
<header>
  <h1>Coordinated Allocation Server</h1>
  <span class="ts">Generated {{ .GeneratedAt }} &bull; live updates via SSE</span>
</header>
<main>

{{/* ----- Live State ----- */}}
<div class="card">
  <h2>Live State</h2>
  {{with .Live}}
  <div class="live-grid">
    <div class="live-item" data-field="window">
      <div class="v {{if eq .WindowDisplay "—"}}status-warn{{else if eq .WindowDisplay "N/A"}}status-warn{{else}}status-ok{{end}}">{{if .WindowEndUnix}}<span data-window-end="{{.WindowEndUnix}}">{{.WindowDisplay}}</span>{{else}}{{.WindowDisplay}}{{end}}</div>
      <div class="k">Window remaining{{if eq .WindowAccuracy "high"}} <span class="tag acc-high">⬤ high accuracy</span>{{end}}</div>
    </div>
    <div class="live-item" data-field="available">
      <div class="v {{if gt .Available 0}}status-ok{{else}}status-err{{end}}">{{.Available}}</div>
      <div class="k">Available slots</div>
    </div>
    <div class="live-item" data-field="remaining">
      <div class="v">{{.LocalRemaining}}</div>
      <div class="k">Local Remaining</div>
    </div>
    <div class="live-item" data-field="inflight">
      <div class="v" style="color:var(--yellow)">{{.InFlight}}</div>
      <div class="k">In-Flight</div>
    </div>
    <div class="live-item" data-field="queue">
      <div class="v">{{.QueueDepth}}</div>
      <div class="k">Queue Depth</div>
    </div>
    <div class="live-item" data-field="lockdown">
      <div class="v {{if .LockdownActive}}status-err{{else}}status-ok{{end}}">{{.LockdownDisplay}}</div>
      <div class="k">Lockdown</div>
    </div>
  </div>
  <div class="live-grid" style="grid-template-columns:repeat(auto-fill,minmax(100px,1fr));margin-top:10px;opacity:.85">
    <div class="live-item" data-field="p1">
      <div class="v" style="font-size:1.1rem;color:#6ab0ff">{{.P1Remaining}}</div>
      <div class="k">P1 budget</div>
    </div>
    <div class="live-item" data-field="p2">
      <div class="v" style="font-size:1.1rem;color:#6adf8a">{{.P2Remaining}}</div>
      <div class="k">P2 budget</div>
    </div>
    <div class="live-item" data-field="p3">
      <div class="v" style="font-size:1.1rem;color:#dfcf6a">{{.P3Remaining}}</div>
      <div class="k">P3 budget</div>
    </div>
    <div class="live-item" data-field="shared">
      <div class="v" style="font-size:1.1rem;color:var(--muted)">{{.SharedRemaining}}</div>
      <div class="k">Shared pool</div>
    </div>
  </div>
  {{end}}
</div>

{{/* ----- Statistics by Period ----- */}}
<div class="card">
  <div class="filter-row">
    <h2 style="margin-bottom:0">Statistics by Period</h2>
    <select class="filter-select" onchange="var v=this.value;if(v)window.location.search='?appliance='+v;else window.location.search=''" data-appliance-filter>
      <option value="">All appliances</option>
      {{range .Appliances}}<option value="{{.}}"{{if eq $.ApplianceFilter .}} selected{{end}}>{{.}}</option>{{end}}
    </select>
  </div>
  <div style="overflow-x:auto">
  <table>
    <thead><tr>
      <th></th>{{range .Periods}}<th>Last {{.Label}}</th>{{end}}
    </tr></thead>
    <tbody>
      <tr><td class="stat-label">Requests</td>{{range .Periods}}<td class="stat-val">{{.Stats.TotalRequests}}</td>{{end}}</tr>
      <tr><td class="stat-label">429</td>{{range .Periods}}<td class="stat-val">{{.Stats.Total429s}}</td>{{end}}</tr>
      <tr><td class="stat-label">4xx (non-429)</td>{{range .Periods}}<td class="stat-val">{{.Stats.Total4xx}}</td>{{end}}</tr>
      <tr><td class="stat-label">5xx</td>{{range .Periods}}<td class="stat-val">{{.Stats.Total5xx}}</td>{{end}}</tr>
      <tr><td class="stat-label">External API</td>{{range .Periods}}<td class="stat-val">{{.Stats.TotalLegacyUsage}}</td>{{end}}</tr>
      <tr><td class="stat-label">Avg Latency (ms)</td>{{range .Periods}}<td class="stat-val">{{printf "%.1f" .Stats.AvgLatencyMs}}</td>{{end}}</tr>
      <tr><td class="stat-label">Throughput /30s</td>{{range .Periods}}<td class="stat-val">{{printf "%.1f" .ThroughputExcl}}</td>{{end}}</tr>
      <tr><td class="stat-label">+ext api /30s</td>{{range .Periods}}<td class="stat-val">{{printf "%.1f" .ThroughputIncl}}</td>{{end}}</tr>
    </tbody>
  </table>
  </div>
</div>

{{/* ----- Recent Requests ----- */}}
<div class="card">
  <div class="filter-row">
    <h2 style="margin-bottom:0">Recent Requests (last 50) &mdash; <a href="/api/logs?n=500">JSON API</a></h2>
    <input type="text" class="filter-input" placeholder="Filter appliance" data-filter="logs">
  </div>
  <div style="overflow-x:auto">
  <table>
    <thead><tr>
      <th>Age</th><th>Appliance</th><th>Priority</th><th>Status</th>
      <th>Remaining</th><th>Latency ms</th><th>External API</th>
      <th>Acquired At</th><th>Token</th>
    </tr></thead>
    <tbody>
    {{range .Logs}}
    <tr data-appliance="{{.ApplianceID}}">
      <td style="color:var(--muted)" data-time="{{.AcquiredAt}}">{{.ID}}</td>
      <td>{{.ApplianceID}}</td>
      <td>
        {{if eq .PriorityClass "P1_HIGH"}}<span class="tag p1">P1</span>
        {{else if eq .PriorityClass "P2_MEDIUM"}}<span class="tag p2">P2</span>
        {{else}}<span class="tag p3">P3</span>{{end}}
      </td>
      <td>
        {{if .StatusCode}}
          {{if eq (derefInt .StatusCode) 200}}<span class="status-ok">200</span>
          {{else if eq (derefInt .StatusCode) 429}}<span class="status-err">429</span>
          {{else}}<span class="status-warn">{{derefInt .StatusCode}}</span>{{end}}
        {{else}}<span class="status-warn">?</span>{{end}}
      </td>
      <td>{{if .NSRemaining}}{{derefInt .NSRemaining}}{{else}}-{{end}}</td>
      <td>{{if .EstimatedLatencyMs}}{{printf "%.0f" (derefFloat .EstimatedLatencyMs)}}{{else}}-{{end}}</td>
      <td>
        {{if .ImpliedLegacyUsage}}
          {{if gt (derefInt .ImpliedLegacyUsage) 0}}<span class="status-warn">{{derefInt .ImpliedLegacyUsage}}</span>
          {{else}}0{{end}}
        {{else}}-{{end}}
      </td>
      <td style="color:var(--muted)">{{shortTime .AcquiredAt}}</td>
      <td style="color:var(--muted);font-size:.65rem">{{slice .Token 0 6}}…</td>
    </tr>
    {{else}}<tr><td colspan="9" style="color:var(--muted);text-align:center;padding:20px">No requests recorded yet</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
</div>

{{/* ----- Window Log ----- */}}
<div class="card">
  <h2>Window Detections (last 20) &mdash; <a href="/api/windows">JSON API</a></h2>
  <div style="overflow-x:auto">
  <table>
    <thead><tr><th>Age</th><th>Detected Via</th><th>Window Start</th><th>Window End</th><th>Trigger Token</th><th>Logged At</th></tr></thead>
    <tbody>
    {{range .Windows}}
    <tr>
      <td style="color:var(--muted)" data-time="{{.LoggedAt}}">{{.ID}}</td>
      <td>
        {{if eq .DetectedVia "49_trigger"}}<span class="tag det-primary">49-trigger</span>
        {{else}}<span class="tag det-fallback">fallback</span>{{end}}
      </td>
      <td>{{shortTime .WindowStart}}</td>
      <td>{{shortTime .WindowEnd}}</td>
      <td style="font-size:.65rem;color:var(--muted)">{{if .TriggerToken}}{{slice .TriggerToken 0 6}}…{{else}}-{{end}}</td>
      <td style="color:var(--muted)">{{shortTime .LoggedAt}}</td>
    </tr>
    {{else}}<tr><td colspan="6" style="color:var(--muted);text-align:center;padding:20px">No window detections yet</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
</div>

{{/* ----- Anomalies ----- */}}
<div class="card">
  <div class="filter-row">
    <h2 style="margin-bottom:0">Anomalies (last 20) &mdash; <a href="/api/anomalies">JSON API</a></h2>
    <input type="text" class="filter-input" placeholder="Filter appliance" data-filter="anomalies">
  </div>
  <div style="overflow-x:auto">
  <table>
    <thead><tr><th>Age</th><th>Event</th><th>Appliance</th><th>Token</th><th>Detail</th><th>Logged At</th></tr></thead>
    <tbody>
    {{range .Anomalies}}
    <tr data-appliance="{{.ApplianceID}}">
      <td style="color:var(--muted)" data-time="{{.LoggedAt}}">{{.ID}}</td>
      <td><span class="badge err">{{.EventType}}</span></td>
      <td>{{.ApplianceID}}</td>
      <td style="font-size:.65rem;color:var(--muted)">{{if .Token}}{{slice .Token 0 6}}…{{else}}-{{end}}</td>
      <td style="overflow:hidden;text-overflow:ellipsis">{{.Detail}}</td>
      <td style="color:var(--muted)">{{shortTime .LoggedAt}}</td>
    </tr>
    {{else}}<tr><td colspan="6" style="color:var(--muted);text-align:center;padding:20px">No anomalies recorded</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
</div>

</main>
<script>
(function(){function ago(n){return n<60?n+'s ago':n<3600?Math.floor(n/60)+'m ago':n<86400?Math.floor(n/3600)+'h ago':Math.floor(n/86400)+'d ago'}
function tick(){var n=Date.now()
document.querySelectorAll('[data-window-end]').forEach(function(e){var r=parseInt(e.getAttribute('data-window-end'),10);e.textContent=Math.max(0,Math.floor((r-n)/1000))+'s'})
document.querySelectorAll('[data-time]').forEach(function(e){var s=e.getAttribute('data-time').replace(' ','T'),d=Date.parse(s);if(!isNaN(d))e.textContent=ago(Math.floor((n-d)/1000))})}
tick();setInterval(tick,1000)

var es=new EventSource('/api/events')
es.addEventListener('live',function(e){var d=JSON.parse(e.data)
var wi=document.querySelector('[data-field="window"]');if(wi){var v=wi.querySelector('.v'),k=wi.querySelector('.k')
if(d.windowEndUnix>0){var s=v.querySelector('[data-window-end]');if(!s){v.innerHTML='';s=document.createElement('span');v.appendChild(s)}
s.setAttribute('data-window-end',d.windowEndUnix)}else{v.innerHTML=d.windowDisplay}
v.className='v '+(d.windowDisplay==='—'||d.windowDisplay==='N/A'?'status-warn':'status-ok')
k.innerHTML='Window remaining'+(d.windowAccuracy==='high'?' <span class="tag acc-high">⬤ high accuracy</span>':'')}
var f=function(k){return document.querySelector('[data-field="'+k+'"] .v')}
var av=f('available');if(av){av.textContent=d.available;av.className='v '+(d.available>0?'status-ok':'status-err')}
var re=f('remaining');if(re)re.textContent=d.remaining
var inf=f('inflight');if(inf)inf.textContent=d.inFlight
var qu=f('queue');if(qu)qu.textContent=d.queueDepth
 var lo=document.querySelector('[data-field="lockdown"]');if(lo){var lv=lo.querySelector('.v');lv.textContent=d.lockdownDisplay;lv.className='v '+(d.lockdownActive?'status-err':'status-ok')}
 var p1=document.querySelector('[data-field="p1"] .v');if(p1)p1.textContent=d.p1Remaining
 var p2=document.querySelector('[data-field="p2"] .v');if(p2)p2.textContent=d.p2Remaining
 var p3=document.querySelector('[data-field="p3"] .v');if(p3)p3.textContent=d.p3Remaining
 var sh=document.querySelector('[data-field="shared"] .v');if(sh)sh.textContent=d.sharedRemaining})

function setupFilters(){document.querySelectorAll('.filter-input').forEach(function(inp){inp.removeEventListener('input',inp._filter);function h(){var v=inp.value.toLowerCase(),t=inp.closest('.card').querySelector('table');if(!t)return;t.querySelectorAll('tbody tr[data-appliance]').forEach(function(r){r.style.display=r.getAttribute('data-appliance').toLowerCase().includes(v)?'':'none'})}
inp._filter=h;inp.addEventListener('input',h)})}
setupFilters()

function refreshMain(){var q=window.location.search;fetch('/dashboard'+q).then(function(r){return r.text()}).then(function(h){var m=h.match(/<main>([\s\S]*)<\/main>/i);if(m){document.querySelector('main').innerHTML=m[1];tick();setupFilters()}}).catch(function(){})}
setInterval(refreshMain,30000)})();
</script>
</body>
</html>`
