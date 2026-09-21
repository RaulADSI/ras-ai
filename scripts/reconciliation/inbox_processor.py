"""Procesador de decisiones, con commit por fila y archivo original archivado."""
import csv
import io
import json
import uuid
from pathlib import Path


COLUMNS = {'decision_id', 'exception_id', 'allocation_id', 'action',
           'corrected_fields_json', 'reason', 'reviewed_by'}


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Clave JSON duplicada')
        result[key] = value
    return result


def _read_decisions(content):
    reader = csv.DictReader(io.StringIO(content.decode('utf-8-sig'), newline=''), strict=True)
    names = reader.fieldnames or []
    if len(names) != len(set(names)) or not COLUMNS <= set(names) or set(names) - COLUMNS - {'exception_version'}:
        raise ValueError('Cabecera CSV inválida')
    decisions = []
    for row in reader:
        if None in row or any(value is None for value in row.values()):
            raise ValueError('Fila CSV con columnas faltantes o sobrantes')
        fields = json.loads(row['corrected_fields_json'], object_pairs_hook=_object)
        if not isinstance(fields, dict):
            raise ValueError('corrected_fields_json debe ser un objeto')
        decisions.append(dict(decision_id=row['decision_id'], exception_id=row['exception_id'],
            allocation_id=int(row['allocation_id']), action=row['action'], corrected_fields=fields,
            reason=row['reason'], reviewed_by=row['reviewed_by'],
            exception_version=int(row.get('exception_version', '1'))))
    if not decisions:
        raise ValueError('Archivo sin decisiones')
    return decisions


def process_inbox(ledger, inbox_dir: Path, processed_dir: Path, failed_dir: Path) -> dict:
    """Un consumidor por inbox. Las decisiones previas permanecen ante fallo posterior.

    Los errores se devuelven en summary; un error de archivado deja el archivo en
    inbox para reintento idempotente. Nunca sobrescribe archivos del historial.
    """
    inbox, processed, failed = map(lambda p: Path(p).resolve(), (inbox_dir, processed_dir, failed_dir))
    paths = (inbox, processed, failed)
    if any(a == b or a in b.parents or b in a.parents for i,a in enumerate(paths) for b in paths[i+1:]):
        raise ValueError('Las carpetas deben ser distintas y no estar anidadas')
    processed.mkdir(parents=True, exist_ok=True)
    failed.mkdir(parents=True, exist_ok=True)
    summary = dict(files_processed=0, files_failed=0, archive_errors=0,
                   decisions_applied=0, decisions_skipped=0, errors=[])
    for path in sorted(inbox.glob('*.csv')):
        content = None
        try:
            if path.is_symlink():
                raise ValueError('No se admiten enlaces simbólicos en inbox')
            content = path.read_bytes()
            decisions = _read_decisions(content)  # Parsear todo antes del primer commit.
            for row in decisions:
                status = ledger.apply_decision(**row)
                summary['decisions_applied' if status == 'APPLIED' else 'decisions_skipped'] += 1
        except Exception as exc:
            summary['files_failed'] += 1
            summary['errors'].append(dict(file=path.name, stage='decisions', error=str(exc)))
            target_dir = failed
        else:
            target_dir = processed
        try:
            if content is None or path.read_bytes() != content or path.is_symlink():
                raise ValueError('Archivo no disponible o modificado durante el procesamiento')
            destination = target_dir / f'{path.stem}_{uuid.uuid4().hex}.csv'
            path.rename(destination)
            if target_dir == processed:
                summary['files_processed'] += 1
        except Exception as exc:
            summary['archive_errors'] += 1
            summary['errors'].append(dict(file=path.name, stage='archive', error=str(exc)))
    return summary
