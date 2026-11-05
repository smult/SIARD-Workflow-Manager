"""Standard SIARD Workflow Manager file and PDF reports for Generic Decrypt.

Uses the same column conventions as the existing BLOB conversion reports and
matching A4/nayv-blue/green/red PDF styling. Each result is recorded by row;
passwords themselves are never written to logs or reports.
"""
from __future__ import annotations

import csv
import datetime as dt
import html
from pathlib import Path


CSV_HEADER = (
    'fra_fil', 'fra_storrelse', 'fra_ext', 'til_fil', 'til_storrelse',
    'til_ext', 'kommentar', 'opprinnelig_filnavn', 'passordkilde',
    'formelavvik', 'status',
)
ERROR_HEADER = ('fil', 'plassering', 'format', 'kategori', 'arsak')


def _csv_write(path: Path, headers: tuple[str, ...], rows: list[list]) -> None:
    """BOM, semicolon delimiter and full quoting rules match the converter CSV."""
    with path.open('w', encoding='utf-8-sig', newline='') as fh:
        writer = csv.writer(fh, delimiter=';')
        writer.writerow(headers)
        writer.writerows(rows)


def _paths(log_dir: Path, source_stem: str, log_file: Path | None) -> dict[str, Path]:
    # Reuse the workflow's timestamp so these are unambiguously one run.
    run = ''
    if log_file is not None and log_file.stem.startswith(source_stem + '_'):
        run = log_file.stem[len(source_stem) + 1:]
    if not run:
        run = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix = f'{source_stem}_{run}'
    return {
        'csv': log_dir / f'{prefix}_dokumentopplasing.csv',
        'errors_csv': log_dir / f'{prefix}_dokumentopplasing_feil.csv',
        'errors_log': log_dir / f'{prefix}_dokumentopplasing_feil.log',
        'pdf': log_dir / f'{prefix}_dokumentopplasing_rapport.pdf',
    }


def write_decrypt_reports(
    log_dir: Path, source_stem: str, records: list[dict], stats: dict,
    rule_name: str, *, dry_run: bool, log_file: Path | None = None,
) -> dict[str, str]:
    """Write all report variants. Return paths, or raise with a visible error."""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    paths = _paths(log_dir, source_stem, log_file)
    rows: list[list] = []
    errors: list[list] = []
    for rec in records:
        path = rec['fil']
        status = rec['status']
        comment = rec.get('arsak', '')
        src_ext = Path(path).suffix.lstrip('.').lower()
        rows.append([
            path, rec.get('fra_storrelse', ''), src_ext,
            path, rec.get('til_storrelse', ''), src_ext, comment,
            rec.get('opprinnelig_filnavn', ''),
            rec.get('passordkilde', ''), rec.get('formelavvik', ''), status,
        ])
        if status == 'feil':
            errors.append([
                path, rec.get('plassering', ''), rec.get('format', ''),
                rec.get('kategori', '') or 'Dekryptering feilet', comment,
            ])

    _csv_write(paths['csv'], CSV_HEADER, rows)
    _csv_write(paths['errors_csv'], ERROR_HEADER, errors)
    with paths['errors_log'].open('w', encoding='utf-8') as f:
        f.write(f'Generisk dokumentopplåsing – {source_stem}\n')
        f.write(f'Regelsett: {rule_name}\n')
        f.write(f'Kjøremodus: {"TEST" if dry_run else "DEKRYPTERING"}\n')
        f.write(f'Feil: {len(errors)}\n\n')
        for file, placement, fmt, category, reason in errors:
            f.write(f'{category}: {file} ({placement}, {fmt}) – {reason}\n')

    _write_pdf(paths['pdf'], source_stem, rule_name, stats, errors,
               paths, dry_run=dry_run)
    return {k: str(v) for k, v in paths.items()}


def _write_pdf(pdf_path: Path, source_stem: str, rule_name: str, stats: dict,
               errors: list[list], paths: dict, *, dry_run: bool) -> None:
    """A4 control report in the existing Workflow Manager report visual style."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.graphics.shapes import Drawing, String
    from reportlab.graphics.charts.piecharts import Pie
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        KeepTogether,
    )
    navy = colors.HexColor('#1a3a5c')
    accent = colors.HexColor('#2980b9')
    green = colors.HexColor('#27ae60')
    red = colors.HexColor('#c0392b')
    blue = colors.HexColor('#2980b9')
    gray = colors.HexColor('#7f8c8d')
    light = colors.HexColor('#eaf3fb')
    border = colors.HexColor('#d5d8dc')
    w, h = A4
    inner = w - 4 * cm
    st = getSampleStyleSheet()
    title = ParagraphStyle('DecryptTitle', parent=st['Title'], fontName='Helvetica-Bold',
                           textColor=navy, fontSize=19, leading=24, spaceAfter=8)
    section = ParagraphStyle('DecryptSection', parent=st['Heading2'], fontName='Helvetica-Bold',
                             textColor=navy, fontSize=13, leading=17, spaceBefore=15, spaceAfter=7)
    small = ParagraphStyle('DecryptSmall', parent=st['BodyText'], fontName='Helvetica',
                           textColor=navy, fontSize=8.5, leading=11, splitLongWords=True)
    sub = ParagraphStyle('DecryptSub', parent=small, fontSize=10, leading=13, spaceAfter=5)
    hdr = ParagraphStyle('DecryptHdr', parent=small, fontName='Helvetica-Bold',
                         textColor=colors.white, fontSize=8)
    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4, leftMargin=2 * cm,
                            rightMargin=2 * cm, topMargin=2 * cm,
                            bottomMargin=2 * cm, title='Dokumentopplåsing – kontrollrapport',
                            author='SIARD Workflow Manager')

    def esc(x) -> str:
        return html.escape(str(x or ''))

    def table(headers: list[str], rows: list[list], widths: list[float]) -> Table:
        data = [[Paragraph(esc(c), hdr) for c in headers]]
        data += [[Paragraph(esc(c), small) for c in row] for row in rows]
        t = Table(data, colWidths=widths, repeatRows=1, hAlign='LEFT')
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), navy),
            ('GRID', (0, 0), (-1, -1), 0.4, border),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        return t

    saved = int(stats.get('decrypted_saved', 0))
    tested = int(stats.get('decrypted_tested', 0))
    kept = int(stats.get('not_encrypted', 0)) + int(stats.get('pdf_skipped', 0)) + int(stats.get('format_skipped', 0)) + int(stats.get('already_exists', 0))
    failed = len(errors)
    selected = int(stats.get('selected', 0))
    story = [
        Paragraph('DOKUMENTOPPLÅSING', title),
        Paragraph('SIARD Workflow Manager – Automatisert kontrollrapport', sub),
        Spacer(1, 0.4 * cm),
    ]
    story.append(table(['Opplysning', 'Verdi'], [
        ['SIARD-uttrekk', source_stem],
        ['Kjørt', dt.datetime.now().strftime('%d.%m.%Y kl. %H:%M:%S')],
        ['Regelsett', rule_name],
        ['Modus', 'Testkjøring (ingen endringer)' if dry_run else 'Oppdatert SIARD-arbeidskopi'],
    ], [4 * cm, inner - 4 * cm]))
    story.append(Spacer(1, 0.45 * cm))
    kind = ('FEIL OPPDAGET' if failed else 'GODKJENT')
    status = Table([[kind, f'{saved if not dry_run else tested:,} behandlet, {failed:,} feil']],
                   colWidths=[inner * .48, inner * .52])
    status.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#fadbd8') if failed else colors.HexColor('#d5f5e3')),
        ('BACKGROUND', (1, 0), (1, 0), colors.HexColor('#f4f6f8')),
        ('TEXTCOLOR', (0, 0), (0, 0), red if failed else green),
        ('TEXTCOLOR', (1, 0), (1, 0), navy),
        ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (0, 0), 14),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('TOPPADDING', (0, 0), (-1, -1), 13),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 13),
        ('BOX', (0, 0), (-1, -1), 1.0, red if failed else green),
    ]))
    story.extend([status, Paragraph('Dekrypteringsresultat', section)])
    stats_rows = [
        ['Utvalgte dokumenter', f'{selected:,}'],
        ['Dekryptert og oppdatert', f'{saved:,}'],
        ['Testet uten lagring', f'{tested:,}'],
        ['Beholdt uten endring', f'{kept:,}'],
        ['Feil', f'{failed:,}'],
        ['Lagret passord funnet', f'{int(stats.get("password_stored", 0)):,}'],
        ['Formelavvik', f'{int(stats.get("formula_mismatch", 0)):,}'],
        ['Vellykket fallback', f'{int(stats.get("formula_fallback_success", 0)):,}'],
    ]
    story.append(table(['Resultat', 'Antall'], stats_rows, [inner * .65, inner * .35]))
    values = [float(saved + tested), float(kept), float(failed)]
    if sum(values) > 0:
        story.append(Paragraph('Fordeling av dokumentutfall', section))
        d = Drawing(410, 190)
        pie = Pie()
        pie.x, pie.y = 55, 18
        pie.width = 135
        pie.height = 135
        pie.data = values
        pie.labels = [f'{label}: {n / sum(values) * 100:.1f} %'
                      for label, n in zip(('Dekryptert/testet', 'Beholdt', 'Feil'), values)]
        pie.slices.strokeWidth = 0.5
        pie.slices.strokeColor = colors.white
        pie.slices[0].fillColor = green
        pie.slices[1].fillColor = blue
        pie.slices[2].fillColor = red
        pie.slices.fontSize = 8
        pie.slices.labelRadius = 1.3
        d.add(pie)
        story.append(d)
        story.append(Paragraph('Grønn = dekryptert/testet – Blå = beholdt – Rød = feil', small))
    story.append(Paragraph('Detaljerte dokumentlogger', section))
    story.append(table(['Fil', 'Beskrivelse'], [
        [paths['csv'].name, 'Samtlige behandlede dokumenter og resultat per fil'],
        [paths['errors_csv'].name, 'Alle feil med kategori og årsak'],
        [paths['errors_log'].name, 'Lesbar feillogg'],
    ], [inner * .56, inner * .44]))
    if errors:
        story.append(Paragraph(f'Filer som ikke ble dekryptert ({len(errors):,})', section))
        summary: dict[str, int] = {}
        for err in errors:
            summary[err[3]] = summary.get(err[3], 0) + 1
        story.append(table(['Årsak', 'Antall'], [[k, f'{v:,}'] for k, v in sorted(summary.items())],
                           [inner * .7, inner * .3]))
        # The original conversion report uses a bounded PDF sample with the
        # complete fault list in CSV, avoiding an enormous PDF for big batches.
        limit = 100
        story.append(Paragraph(f'Feildetaljer (første {min(limit, len(errors)):,} av {len(errors):,})', section))
        story.append(table(['Årsak', 'Fil', 'Tabell/kolonne', 'Format', 'Detaljer'],
                           [[e[3], e[0], e[1], e[2], e[4]] for e in errors[:limit]],
                           [inner * .15, inner * .29, inner * .20, inner * .09, inner * .27]))
        if len(errors) > limit:
            story.append(Paragraph('Fullstendig feilliste finnes i CSV-filen.', sub))
    else:
        story.append(Paragraph('Ingen dokumentfeil registrert.', sub))

    def footer(canvas, d):
        canvas.saveState()
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(gray)
        canvas.drawString(2 * cm, 1.0 * cm, 'SIARD Workflow Manager – ' + source_stem[:55])
        canvas.drawRightString(w - 2 * cm, 1.0 * cm, f'Side {d.page}')
        if d.page > 1:
            canvas.setStrokeColor(border)
            canvas.line(2 * cm, h - 1.5 * cm, w - 2 * cm, h - 1.5 * cm)
        canvas.restoreState()
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
