"""
siard_workflow/core/report.py
Genererer HTML- og PDF-rapport fra et WorkflowRun-objekt.
PDF krever ingen eksterne avhengigheter - konverteres fra HTML via webbrowser
eller kan lagres som HTML alene.
"""
from __future__ import annotations
import datetime
from pathlib import Path
from .workflow import WorkflowRun


_CSS = """
body{font-family:'Courier New',monospace;background:#0d0f14;color:#d4daf0;margin:0;padding:32px}
h1{color:#4f8ef7;font-size:1.4em;margin-bottom:4px}
.meta{color:#5a637a;font-size:.85em;margin-bottom:28px}
.summary{border:1px solid #252b3a;border-radius:8px;overflow:hidden;margin-bottom:24px}
.summary-header{background:#13161e;padding:14px 20px;display:flex;justify-content:space-between;align-items:center}
.badge{padding:4px 12px;border-radius:4px;font-size:.8em;font-weight:700}
.badge-ok{background:#1a3d2b;color:#2ecc71;border:1px solid #2ecc7166}
.badge-feil{background:#3d1a1a;color:#e05252;border:1px solid #e0525266}
table{width:100%;border-collapse:collapse}
th{background:#191d28;color:#7a849e;font-size:.75em;text-transform:uppercase;
   letter-spacing:1px;padding:10px 16px;text-align:left}
td{padding:10px 16px;border-top:1px solid #252b3a;font-size:.88em;vertical-align:top}
.ok{color:#2ecc71}.feil{color:#e05252}.skip{color:#5a637a}
.data-block{background:#191d28;border-radius:4px;padding:8px 12px;
            font-size:.8em;color:#7a849e;margin-top:4px;white-space:pre-wrap;word-break:break-all}
.section-title{color:#5a637a;font-size:.75em;text-transform:uppercase;
               letter-spacing:2px;margin:24px 0 8px}
"""

def _data_html(data: dict) -> str:
    if not data:
        return ""
    rows = "\n".join(f"  {k}: {v}" for k, v in data.items())
    return f'<div class="data-block">{rows}</div>'


def generate_html(run: WorkflowRun, workflow_name: str = "") -> str:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    overall = "SUKSESS" if run.success else "FEIL"
    badge_cls = "badge-ok" if run.success else "badge-feil"

    rows_html = ""
    for r in run.results:
        icon = "✓" if r.success else "✗"
        cls  = "ok" if r.success else "feil"
        rows_html += f"""
        <tr>
          <td class="{cls}">{icon}</td>
          <td>{r.operation_id}</td>
          <td>{r.message}</td>
          <td>{_data_html(r.data)}</td>
        </tr>"""

    for s in run.skipped:
        rows_html += f"""
        <tr>
          <td class="skip">–</td>
          <td>{s}</td>
          <td class="skip">Hoppet over</td>
          <td></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="no">
<head>
  <meta charset="UTF-8">
  <title>SIARD Rapport — {run.siard_path.name}</title>
  <style>{_CSS}</style>
</head>
<body>
  <h1>SIARD Workflow Rapport</h1>
  <div class="meta">
    Fil: {run.siard_path}<br>
    Workflow: {workflow_name or run.siard_path.stem}<br>
    Tidspunkt: {ts}<br>
    Varighet: {run.elapsed:.2f}s
  </div>

  <div class="summary">
    <div class="summary-header">
      <span>Resultat</span>
      <span class="badge {badge_cls}">{overall}</span>
    </div>
    <table>
      <thead>
        <tr>
          <th style="width:30px"></th>
          <th>Operasjon</th>
          <th>Melding</th>
          <th>Data</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</body>
</html>"""


def save_html(run: WorkflowRun, output_path: Path, workflow_name: str = "") -> Path:
    html = generate_html(run, workflow_name)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def save_pdf(run: WorkflowRun, output_path: Path, workflow_name: str = "") -> Path:
    """
    Lager PDF via weasyprint hvis installert, ellers HTML.
    Returnerer faktisk lagret sti (kan ende pa .html hvis weasyprint mangler).
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
