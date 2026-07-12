// Package monitor serves the live HTTP observability dashboard: a
// single-page app fed by a Server-Sent-Events stream of command activity
// and periodic stat snapshots. It reads from an events.Recorder only.
package monitor

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"redis-glass/events"
)

// Stats is the periodic snapshot of server-wide metrics pushed to the dashboard.
type Stats struct {
	Uptime          string `json:"uptime"`
	Conns           int64  `json:"conns"`
	Commands        int64  `json:"commands"`
	OpsPerSec       int64  `json:"opsPerSec"`
	Keys            int    `json:"keys"`
	Expires         int    `json:"expires"`
	AOFPath         string `json:"aofPath"`
	GoVersion       string `json:"goVersion"`
	MemoryUsedBytes int64  `json:"memoryUsedBytes"`
	MemoryMaxBytes  int64  `json:"memoryMaxBytes"`
}

// Sources bundles what the dashboard needs to read. Events is optional —
// if nil, the live command stream, latency chart, slow log, and hotkeys
// panels simply stay empty (no error, no special-casing required of callers).
//
// Password, if non-empty, requires HTTP Basic Auth on every dashboard route
// (username is ignored, only the password is checked). If empty, the
// dashboard is unauthenticated — the same "opt-in, backward compatible"
// pattern the rest of the server follows, but be aware that the dashboard
// exposes live command data (keys, and values unless SENSITIVE_KEYS is set)
// to anyone who can reach the port, so leaving it empty on anything but a
// local/trusted network is a real exposure, not a convenience.
type Sources struct {
	GetStats func() Stats
	Events   *events.Recorder
	Password string
}

// StartDashboard serves the live monitoring SPA on 0.0.0.0:port. "/" serves
// the page shell once; "/monitor" is a Server-Sent-Events stream the page's
// own JS subscribes to for everything after that — no polling, no meta-refresh.
func StartDashboard(port string, src Sources) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(pageHTML))
	})
	mux.HandleFunc("/monitor", func(w http.ResponseWriter, r *http.Request) {
		serveMonitorStream(w, r, src)
	})

	var handler http.Handler = mux
	if src.Password != "" {
		handler = requireAuth(src.Password, mux)
		log.Println("Dashboard auth enabled")
	} else {
		log.Println("Dashboard auth disabled (set DASHBOARD_PASSWORD to require login)")
	}

	addr := "0.0.0.0:" + port
	log.Printf("Dashboard listening on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Printf("dashboard error: %v", err)
	}
}

// requireAuth wraps next with HTTP Basic Auth, checking only the password
// (username is accepted but ignored) using a constant-time comparison so a
// failed attempt can't be timed to learn how many characters matched.
func requireAuth(password string, next http.Handler) http.Handler {
	// Comparing SHA-256 sums (fixed-length) rather than the raw password
	// avoids leaking the password's length via ConstantTimeCompare, which
	// requires equal-length inputs and would otherwise need a length check
	// up front — an easy, common way to accidentally reintroduce a timing leak.
	want := sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pass, ok := r.BasicAuth()
		got := sha256.Sum256([]byte(pass))
		if !ok || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="redis-glass dashboard"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statsPayload struct {
	Type         string               `json:"type"`
	Stats        Stats                `json:"stats"`
	Slowlog      []events.SlowEntry   `json:"slowlog"`
	HotKeys      []events.HotKey      `json:"hotkeys"`
	CommandStats []events.CommandStat `json:"commandStats"`
}

type cmdPayload struct {
	Type  string       `json:"type"`
	Event events.Event `json:"event"`
}

// serveMonitorStream holds one SSE connection open, pushing a "cmd" message
// per live command execution (subscribed via events.Recorder) and a "stats"
// message once per second with the full snapshot (stats, slow log, hotkeys,
// per-command latency) — everything the page needs, over one connection.
func serveMonitorStream(w http.ResponseWriter, r *http.Request, src Sources) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// A nil channel is fine here: a receive on it in the select below simply
	// never fires, which is exactly "no live command stream" when events is
	// disabled — no special-casing needed in the loop itself.
	var evtCh <-chan events.Event
	if src.Events != nil {
		var unsubscribe func()
		evtCh, unsubscribe = src.Events.Subscribe()
		defer unsubscribe()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCommands int64
	sendStats := func() {
		st := Stats{}
		if src.GetStats != nil {
			st = src.GetStats()
		}
		opsPerSec := st.Commands - lastCommands
		if lastCommands == 0 {
			opsPerSec = 0 // first tick has no prior sample to diff against
		}
		st.OpsPerSec = opsPerSec
		lastCommands = st.Commands

		payload := statsPayload{Type: "stats", Stats: st}
		if src.Events != nil {
			payload.Slowlog = src.Events.SlowlogGet(20)
			payload.HotKeys = src.Events.HotKeys(10)
			payload.CommandStats = src.Events.CommandStats()
		}
		if writeSSE(w, payload) {
			flusher.Flush()
		}
	}

	sendStats() // initial snapshot immediately, don't make the page wait a full second

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-evtCh:
			if !ok {
				evtCh = nil
				continue
			}
			if writeSSE(w, cmdPayload{Type: "cmd", Event: evt}) {
				flusher.Flush()
			}
		case <-ticker.C:
			sendStats()
		}
	}
}

func writeSSE(w http.ResponseWriter, v interface{}) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return false
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	return err == nil
}

const pageHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>redis-glass monitor</title>
<style>
  :root {
    --bg: #0d0f14; --panel: #151822; --border: #262b38; --text: #e6e6e6;
    --dim: #8891a3; --accent: #4caf50; --warn: #e0a030; --bad: #e05252;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg); color: var(--text); font-family: ui-monospace, monospace;
    margin: 0; padding: 1.5rem;
  }
  h1 { color: var(--accent); font-weight: 600; margin: 0 0 1rem 0; font-size: 1.4rem; }
  #dot { display: inline-block; width: 0.6rem; height: 0.6rem; border-radius: 50%; background: var(--bad); margin-left: 0.5rem; }
  .grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem;
  }
  .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; overflow: hidden; }
  .panel h2 { margin: 0 0 0.75rem 0; font-size: 0.95rem; color: var(--dim); text-transform: uppercase; letter-spacing: 0.05em; }
  .panel-wide { grid-column: 1 / -1; }
  table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
  td, th { padding: 0.3rem 0.4rem; text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--dim); font-weight: 500; }
  .stat-row { display: flex; justify-content: space-between; padding: 0.25rem 0; font-size: 0.88rem; }
  .stat-row .label { color: var(--dim); }
  .bar-track { background: #1f232e; border-radius: 4px; height: 0.5rem; overflow: hidden; margin-top: 0.2rem; }
  .bar-fill { background: var(--accent); height: 100%; }
  .bar-fill.warn { background: var(--warn); }
  .bar-fill.bad { background: var(--bad); }
  .hk-row { display: flex; align-items: center; gap: 0.5rem; font-size: 0.82rem; margin-bottom: 0.35rem; }
  .hk-key { flex: 0 0 40%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .hk-bar-track { flex: 1; background: #1f232e; border-radius: 4px; height: 0.6rem; overflow: hidden; }
  .hk-bar-fill { background: var(--accent); height: 100%; }
  .hk-count { flex: 0 0 3rem; text-align: right; color: var(--dim); }
  #stream { max-height: 360px; overflow-y: auto; font-size: 0.8rem; }
  .stream-row { display: flex; gap: 0.6rem; padding: 0.2rem 0; border-bottom: 1px solid var(--border); white-space: nowrap; overflow: hidden; }
  .stream-time { color: var(--dim); flex: 0 0 6.5rem; }
  .stream-cmd { color: var(--accent); flex: 0 0 6rem; font-weight: 600; }
  .stream-args { color: var(--text); flex: 1; overflow: hidden; text-overflow: ellipsis; }
  .stream-dur { color: var(--dim); flex: 0 0 4.5rem; text-align: right; }
  .stream-dur.warn { color: var(--warn); }
  .stream-dur.bad { color: var(--bad); }
  .empty { color: var(--dim); font-size: 0.82rem; }
</style>
</head>
<body>
<h1>redis-glass monitor<span id="dot"></span></h1>
<div class="grid">

  <div class="panel">
    <h2>Server</h2>
    <div class="stat-row"><span class="label">Uptime</span><span id="s-uptime">-</span></div>
    <div class="stat-row"><span class="label">Connections</span><span id="s-conns">-</span></div>
    <div class="stat-row"><span class="label">Ops/sec</span><span id="s-ops">-</span></div>
    <div class="stat-row"><span class="label">Commands processed</span><span id="s-commands">-</span></div>
    <div class="stat-row"><span class="label">Keys / with expiry</span><span id="s-keys">-</span></div>
    <div class="stat-row"><span class="label">AOF path</span><span id="s-aof">-</span></div>
    <div class="stat-row"><span class="label">Go version</span><span id="s-go">-</span></div>
    <div class="stat-row" style="margin-top:0.5rem;"><span class="label">Memory</span><span id="s-mem-text">-</span></div>
    <div class="bar-track"><div class="bar-fill" id="s-mem-bar" style="width:0%"></div></div>
  </div>

  <div class="panel">
    <h2>Hot keys (last ~60s)</h2>
    <div id="hotkeys"><div class="empty">no activity yet</div></div>
  </div>

  <div class="panel panel-wide">
    <h2>Per-command latency</h2>
    <table>
      <thead><tr><th>command</th><th>calls</th><th>avg</th><th>p50</th><th>p95</th><th>p99</th></tr></thead>
      <tbody id="latency-body"><tr><td colspan="6" class="empty">no commands recorded yet</td></tr></tbody>
    </table>
  </div>

  <div class="panel panel-wide">
    <h2>Slow query log</h2>
    <table>
      <thead><tr><th>time</th><th>duration</th><th>command</th><th>client</th></tr></thead>
      <tbody id="slowlog-body"><tr><td colspan="4" class="empty">no slow commands recorded yet</td></tr></tbody>
    </table>
  </div>

  <div class="panel panel-wide">
    <h2>Live command stream</h2>
    <div id="stream"><div class="empty">waiting for commands...</div></div>
  </div>

</div>
<script>
(function() {
  var STREAM_CAP = 60;
  var streamCount = 0;

  function fmtBytes(n) {
    if (n <= 0) return "0 B";
    var units = ["B","KB","MB","GB"];
    var i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return n.toFixed(1) + " " + units[i];
  }

  function fmtUs(us) {
    if (us >= 1000) return (us / 1000).toFixed(2) + "ms";
    return us + "us";
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function renderStats(st) {
    document.getElementById("s-uptime").textContent = st.uptime;
    document.getElementById("s-conns").textContent = st.conns;
    document.getElementById("s-ops").textContent = st.opsPerSec;
    document.getElementById("s-commands").textContent = st.commands;
    document.getElementById("s-keys").textContent = st.keys + " / " + st.expires;
    document.getElementById("s-aof").textContent = st.aofPath;
    document.getElementById("s-go").textContent = st.goVersion;

    var bar = document.getElementById("s-mem-bar");
    var text = document.getElementById("s-mem-text");
    if (st.memoryMaxBytes > 0) {
      var pct = Math.min(100, (st.memoryUsedBytes / st.memoryMaxBytes) * 100);
      bar.style.width = pct.toFixed(1) + "%";
      bar.className = "bar-fill" + (pct > 90 ? " bad" : pct > 70 ? " warn" : "");
      text.textContent = fmtBytes(st.memoryUsedBytes) + " / " + fmtBytes(st.memoryMaxBytes);
    } else {
      bar.style.width = "0%";
      text.textContent = fmtBytes(st.memoryUsedBytes) + " / unlimited";
    }
  }

  function renderHotkeys(hotkeys) {
    var el = document.getElementById("hotkeys");
    if (!hotkeys || hotkeys.length === 0) {
      el.innerHTML = '<div class="empty">no activity yet</div>';
      return;
    }
    var max = hotkeys[0].count || 1;
    var rows = [];
    for (var i = 0; i < hotkeys.length; i++) {
      var hk = hotkeys[i];
      var pct = Math.max(2, (hk.count / max) * 100);
      rows.push(
        '<div class="hk-row"><span class="hk-key" title="' + escapeHtml(hk.key) + '">' + escapeHtml(hk.key) +
        '</span><div class="hk-bar-track"><div class="hk-bar-fill" style="width:' + pct + '%"></div></div>' +
        '<span class="hk-count">' + hk.count + '</span></div>'
      );
    }
    el.innerHTML = rows.join("");
  }

  function renderLatency(stats) {
    var body = document.getElementById("latency-body");
    if (!stats || stats.length === 0) {
      body.innerHTML = '<tr><td colspan="6" class="empty">no commands recorded yet</td></tr>';
      return;
    }
    var rows = [];
    for (var i = 0; i < stats.length; i++) {
      var s = stats[i];
      var avg = s.calls > 0 ? (s.totalUs / s.calls) : 0;
      rows.push(
        "<tr><td>" + escapeHtml(s.command) + "</td><td>" + s.calls + "</td><td>" +
        fmtUs(avg) + "</td><td>" + fmtUs(s.p50Us) + "</td><td>" + fmtUs(s.p95Us) + "</td><td>" + fmtUs(s.p99Us) + "</td></tr>"
      );
    }
    body.innerHTML = rows.join("");
  }

  function renderSlowlog(entries) {
    var body = document.getElementById("slowlog-body");
    if (!entries || entries.length === 0) {
      body.innerHTML = '<tr><td colspan="4" class="empty">no slow commands recorded yet</td></tr>';
      return;
    }
    var rows = [];
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i];
      var when = new Date(e.time).toLocaleTimeString();
      var cmdline = e.command + " " + (e.args || []).join(" ");
      rows.push(
        "<tr><td>" + when + "</td><td>" + fmtUs(e.durationUs) + "</td><td>" +
        escapeHtml(cmdline) + "</td><td>" + escapeHtml(e.clientAddr) + "</td></tr>"
      );
    }
    body.innerHTML = rows.join("");
  }

  function appendCommand(evt) {
    var el = document.getElementById("stream");
    if (streamCount === 0) { el.innerHTML = ""; }
    var when = new Date(evt.time).toLocaleTimeString();
    var durClass = evt.durationUs >= 100000 ? "bad" : evt.durationUs >= 10000 ? "warn" : "";
    var argline = (evt.args || []).join(" ");
    var row = document.createElement("div");
    row.className = "stream-row";
    row.innerHTML =
      '<span class="stream-time">' + when + '</span>' +
      '<span class="stream-cmd">' + escapeHtml(evt.command) + '</span>' +
      '<span class="stream-args">' + escapeHtml(argline) + '</span>' +
      '<span class="stream-dur ' + durClass + '">' + fmtUs(evt.durationUs) + '</span>';
    el.insertBefore(row, el.firstChild);
    streamCount++;
    while (el.children.length > STREAM_CAP) {
      el.removeChild(el.lastChild);
    }
  }

  function connect() {
    var es = new EventSource("/monitor");
    var dot = document.getElementById("dot");
    es.onopen = function() { dot.style.background = "#4caf50"; };
    es.onerror = function() { dot.style.background = "#e05252"; };
    es.onmessage = function(ev) {
      var msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg.type === "stats") {
        renderStats(msg.stats);
        renderHotkeys(msg.hotkeys);
        renderLatency(msg.commandStats);
        renderSlowlog(msg.slowlog);
      } else if (msg.type === "cmd") {
        appendCommand(msg.event);
      }
    };
  }

  connect();
})();
</script>
</body>
</html>`
