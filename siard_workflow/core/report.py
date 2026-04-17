"""
siard_workflow/core/report.py
Genererer HTML-rapport fra et WorkflowRun-objekt.
"""
from __future__ import annotations
import datetime
import html as _html_mod
from pathlib import Path
from .workflow import WorkflowRun


# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

_CSS = """
:root {
  --bg:       #f1f4f9;
  --surface:  #ffffff;
  --primary:  #1a3a6b;
  --accent:   #2563eb;
  --accent-l: #dbeafe;
  --ok:       #16a34a;
  --ok-l:     #dcfce7;
  --ok-b:     #86efac;
  --err:      #dc2626;
  --err-l:    #fee2e2;
  --err-b:    #fca5a5;
  --skip:     #6b7280;
  --skip-l:   #f3f4f6;
  --border:   #e2e8f0;
  --text:     #1e293b;
  --text-sub: #64748b;
  --mono:     'Cascadia Code','Consolas','Courier New',monospace;
}
*, *::before, *::after { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg);
  color: var(--text);
  margin: 0;
  padding: 28px 32px;
  font-size: 14px;
  line-height: 1.5;
}

/* ── Header ── */
.report-header {
  background: linear-gradient(135deg, #1a3a6b 0%, #2563eb 100%);
  color: #fff;
  border-radius: 14px;
  padding: 28px 32px;
  margin-bottom: 24px;
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}
.report-header h1 {
  font-size: 1.5em;
  font-weight: 700;
  letter-spacing: -0.3px;
  margin-bottom: 4px;
}
.report-header .file-path {
  font-family: var(--mono);
  font-size: .82em;
  opacity: .75;
  word-break: break-all;
}
.report-header .meta-right {
  text-align: right;
  white-space: nowrap;
  font-size: .85em;
  opacity: .85;
  flex-shrink: 0;
}
.report-header .meta-right strong { display: block; font-size: 1.15em; }

/* ── Overall badge ── */
.overall-badge {
  display: inline-block;
  padding: 6px 18px;
  border-radius: 99px;
  font-weight: 700;
  font-size: .9em;
  letter-spacing: .5px;
  margin-top: 10px;
}
.overall-badge.ok   { background: var(--ok-l);  color: var(--ok);  border: 1.5px solid var(--ok-b);  }
.overall-badge.feil { background: var(--err-l); color: var(--err); border: 1.5px solid var(--err-b); }

/* ── Stat-bokser ── */
.stat-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 12px;
  margin-bottom: 24px;
}
.stat-box {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px 20px;
  text-align: center;
}
.stat-box .val {
  font-size: 2em;
  font-weight: 700;
  line-height: 1.1;
}
.stat-box .lbl {
  font-size: .78em;
  color: var(--text-sub);
  text-transform: uppercase;
  letter-spacing: .8px;
  margin-top: 4px;
}
.stat-box.ok   .val { color: var(--ok);   }
.stat-box.err  .val { color: var(--err);  }
.stat-box.skip .val { color: var(--skip); }
.stat-box.time .val { color: var(--accent); }

/* ── Operasjons-liste ── */
.section-title {
  font-size: .72em;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-sub);
  margin: 0 0 10px;
}
.op-list { display: flex; flex-direction: column; gap: 8px; margin-bottom: 28px; }
.op-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
}
.op-card.ok-card   { border-left: 4px solid var(--ok);   }
.op-card.err-card  { border-left: 4px solid var(--err);  }
.op-card.skip-card { border-left: 4px solid var(--skip); }

.op-header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 18px;
  cursor: pointer;
  user-select: none;
}
.op-header:hover { background: #f8fafc; }
.op-icon {
  width: 28px; height: 28px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: .9em; font-weight: 700;
  flex-shrink: 0;
}
.op-icon.ok   { background: var(--ok-l);  color: var(--ok);   }
.op-icon.err  { background: var(--err-l); color: var(--err);  }
.op-icon.skip { background: var(--skip-l); color: var(--skip); }

.op-id {
  font-family: var(--mono);
  font-size: .82em;
  color: var(--text-sub);
  flex-shrink: 0;
  min-width: 160px;
}
.op-msg { flex: 1; font-size: .92em; }
.op-msg.err  { color: var(--err);  }
.op-msg.skip { color: var(--skip); font-style: italic; }

.op-toggle {
  font-size: .8em; color: var(--text-sub);
  flex-shrink: 0;
  transition: transform .15s;
}
.op-toggle.open { transform: rotate(180deg); }

/* ── Data-blokk ── */
.op-data {
  display: none;
  border-top: 1px solid var(--border);
  background: #f8fafc;
  padding: 0;
}
.op-data.open { display: block; }
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: .83em;
  font-family: var(--mono);
}
.data-table td {
  padding: 7px 18px;
  border-top: 1px solid var(--border);
  vertical-align: top;
}
.data-table tr:first-child td { border-top: none; }
.data-table td:first-child {
  color: var(--text-sub);
  white-space: nowrap;
  width: 1%;
  padding-right: 28px;
  font-weight: 600;
}
.data-table td:last-child {
  word-break: break-all;
  color: var(--text);
}

/* ── Nested data (lister) ── */
.nested-key { color: var(--accent); font-weight: 600; }
.data-list  { list-style: none; padding: 0; margin: 0; }
.data-list li::before { content: "• "; color: var(--text-sub); }

/* ── Footer ── */
.report-footer {
  margin-top: 32px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
  font-size: .78em;
  color: var(--text-sub);
  display: flex;
  justify-content: space-between;
}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Hjelpefunksjoner
# ─────────────────────────────────────────────────────────────────────────────

def _esc(s) -> str:
    return _html_mod.escape(str(s)) if s is not None else ""


def _fmt_val(v) -> str:
    """Formater en verdi til HTML. Lister og dicts vises strukturert."""
    if v is None:
        return '<span style="color:var(--text-sub)">—</span>'
    if isinstance(v, bool):
        return "Ja" if v else "Nei"
    if isinstance(v, list):
        if not v:
            return '<span style="color:var(--text-sub)">(tom)</span>'
        items = "".join(f"<li>{_esc(i)}</li>" for i in v)
        return f'<ul class="data-list">{items}</ul>'
    if isinstance(v, dict):
        rows = "".join(
            f'<tr><td class="nested-key">{_esc(k)}</td><td>{_fmt_val(dv)}</td></tr>'
            for k, dv in v.items()
        )
        return f'<table class="data-table" style="margin:0">{rows}</table>'
    s = str(v)
    # Lange strenger: bryt pent
    if len(s) > 120:
        s = s[:118] + "…"
    return _esc(s)


def _data_section(data: dict, card_id: str) -> tuple[str, str]:
    """
    Returnerer (toggle-knapp-html, data-div-html).
    Tomme data → begge tomme strenger.
    """
    if not data:
        return "", ""

    rows = "".join(
        f"<tr><td>{_esc(k)}</td><td>{_fmt_val(v)}</td></tr>"
        for k, v in data.items()
        if not k.startswith("_")
    )
    if not rows:
        return "", ""

    toggle = (
        f'<span class="op-toggle" id="tog-{card_id}" '
        f'onclick="toggle(\'{card_id}\')" title="Vis/skjul data">▾</span>'
    )
    div = (
        f'<div class="op-data" id="dat-{card_id}">'
        f'<table class="data-table">{rows}</table></div>'
    )
    return toggle, div


_JS = """
function toggle(id) {
  var d = document.getElementById('dat-' + id);
  var t = document.getElementById('tog-' + id);
  if (!d) return;
  d.classList.toggle('open');
  t.classList.toggle('open');
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# Hoved-generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(run: WorkflowRun, workflow_name: str = "") -> str:
    ts   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    name = workflow_name or run.siard_path.stem

    n_ok   = sum(1 for r in run.results if r.success)
    n_err  = sum(1 for r in run.results if not r.success)
    n_skip = len(run.skipped)
    elapsed_str = f"{run.elapsed:.2f} s"
    overall = "SUKSESS" if run.success else "FEIL"
    badge_cls = "ok" if run.success else "feil"

    # ── Header ──
    header = f"""
<div class="report-header">
  <div>
    <h1>SIARD Workflow Rapport</h1>
    <div class="file-path">{_esc(run.siard_path)}</div>
    <span class="overall-badge {badge_cls}">{overall}</span>
  </div>
  <div class="meta-right">
    <strong>{_esc(name)}</strong>
    {_esc(ts)}<br>
    Varighet: {elapsed_str}
  </div>
</div>"""

    # ── Stat-bokser ──
    stats = f"""
<div class="stat-row">
  <div class="stat-box ok">
    <div class="val">{n_ok}</div>
    <div class="lbl">Fullført</div>
  </div>
  <div class="stat-box err">
    <div class="val">{n_err}</div>
    <div class="lbl">Feil</div>
  </div>
  <div class="stat-box skip">
    <div class="val">{n_skip}</div>
    <div class="lbl">Hoppet over</div>
  </div>
  <div class="stat-box time">
    <div class="val">{elapsed_str}</div>
    <div class="lbl">Varighet</div>
  </div>
</div>"""

    # ── Operasjons-kort ──
    cards = ""
    for i, r in enumerate(run.results):
        cid   = f"op{i}"
        ok    = r.success
        cls   = "ok" if ok else "err"
        icon  = "✓" if ok else "✗"
        msg_cls = "" if ok else "err"
        toggle, data_div = _data_section(r.data or {}, cid)

        cards += f"""
  <div class="op-card {cls}-card">
    <div class="op-header" onclick="toggle('{cid}')">
      <div class="op-icon {cls}">{icon}</div>
      <div class="op-id">{_esc(r.operation_id)}</div>
      <div class="op-msg {msg_cls}">{_esc(r.message)}</div>
      {toggle}
    </div>
    {data_div}
  </div>"""

    for s in run.skipped:
        cards += f"""
  <div class="op-card skip-card">
    <div class="op-header">
      <div class="op-icon skip">–</div>
      <div class="op-id">{_esc(s)}</div>
      <div class="op-msg skip">Hoppet over (betingelse ikke oppfylt)</div>
    </div>
  </div>"""

    ops_section = f"""
<p class="section-title">Operasjoner ({len(run.results) + n_skip} totalt)</p>
<div class="op-list">{cards}
</div>"""

    # ── Footer ──
    footer = f"""
<div class="report-footer">
  <span>KDRS SIARD Workflow Manager</span>
  <span>Generert: {_esc(ts)}</span>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="no">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SIARD Rapport — {_esc(run.siard_path.name)}</title>
  <style>{_CSS}</style>
</head>
<body>
{header}
{stats}
{ops_section}
{footer}
<script>{_JS}</script>
</body>
</html>"""


def save_html(run: WorkflowRun, output_path: Path, workflow_name: str = "") -> Path:
    html = generate_html(run, workflow_name)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def save_pdf(run: WorkflowRun, output_path: Path, workflow_name: str = "") -> Path:
    """
    Lager PDF via weasyprint hvis installert, ellers HTML.
    Returnerer faktisk lagret sti.
    """
    html = generate_html(run, workflow_name)
    try:
        from weasyprint import HTML as WP
        WP(string=html).write_pdf(str(output_path))
        return output_path
    except ImportError:
        html_path = output_path.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
        return html_path
