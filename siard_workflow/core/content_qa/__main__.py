"""CLI for the generic content QA pilot: python -m siard_workflow.core.content_qa FILE.siard"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .engine import run_content_qa
from .profile import load_profile
from .reporting import write_qa_reports


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Skrivebeskyttet SIARD innholdskontroll')
    parser.add_argument('siard', help='SIARD ZIP-fil eller utpakket SIARD-mappe')
    parser.add_argument('--profile', default='', help='QA-mal i JSON (tom = ePhorte-kartleggingsmal)')
    parser.add_argument('--out', default='', help='Utmappe for CSV, JSON og PDF (tom = ved SIARD)')
    parser.add_argument('--skip-metadata-pk', action='store_true',
                        help='Ikke kontroller deklarerte primærnøkler')
    parser.add_argument('--metadata-only', '--kartlegging', dest='metadata_only',
                        action='store_true',
                        help='Les kun katalog og lag kartleggingsforslag; ikke radskann')
    args = parser.parse_args(argv)
    try:
        source = Path(args.siard)
        profile = load_profile(args.profile)
        result = run_content_qa(source, profile, scan_rows=not args.metadata_only,
                                check_metadata_pk=not args.skip_metadata_pk,
                                on_progress=lambda msg: print(msg, flush=True))
        output = Path(args.out) if args.out else source.parent
        paths = write_qa_reports(result, output, source.stem)
        print(f'QA-status: {result["status"]}')
        for k, val in paths.items():
            print(f'  {k}: {val}')
        return {'GODKJENT': 0, 'DELVIS_KONTROLLERT': 2, 'AVVIK': 3}[result['status']]
    except (OSError, ValueError, ImportError, RuntimeError) as exc:
        print(f'QA-feil: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
