"""Informe HTML de propiedades pendientes, generado desde el plan de reglas."""
import argparse
import html
import json
from scripts.config import ENTITY_DICTIONARY
from pathlib import Path


def load_property_options(dictionary_path=ENTITY_DICTIONARY):
    """Lee únicamente los nombres de propiedad del catálogo actualizado."""
    from openpyxl import load_workbook
    workbook = load_workbook(dictionary_path, read_only=True, data_only=True)
    try:
        sheet = workbook['property_directory']
        rows = iter(sheet.values)
        headers = [str(value or '').strip().lower() for value in next(rows)]
        if 'property' not in headers:
            raise ValueError('Falta la columna property en property_directory')
        column = headers.index('property')
        names = {}
        for row in rows:
            value = row[column]
            if value is None or not str(value).strip():
                continue
            value = str(value).strip()
            names.setdefault(value.casefold(), value)
        if not names:
            raise ValueError('El catálogo de propiedades está vacío')
        return sorted(names.values(), key=str.casefold)
    finally:
        workbook.close()


def money(cents):
    sign = '-' if cents < 0 else ''
    whole, fraction = divmod(abs(cents), 100)
    return sign + f'{whole:,}.{fraction:02d}'


def build_unresolved_properties_report(plan, destination, *, currency='USD', source_name='AMEX', property_options=None):
    rows = [r for r in plan['details'] if r.get('eligibility_state') != 'MATCHED_EXISTING'
            and ('UNRESOLVED_PROPERTY' in r.get('flags', [])
                 or r.get('property_resolved', '').startswith('REVISAR PROP:'))]
    rows.sort(key=lambda r: (r['date'], r['row_number']))
    ids = [r['provenance_hash'] for r in rows]
    if len(ids) != len(set(ids)) or any(type(r['amount_cents']) is not int for r in rows):
        raise ValueError('Identidades duplicadas o importes no enteros')
    total = sum(r['amount_cents'] for r in rows)
    esc = lambda value: html.escape(str(value), quote=True)
    options = load_property_options() if property_options is None else property_options
    options_html = '<option value="">Select a property</option>' + ''.join(
        f'<option value="{esc(name)}">{esc(name)}</option>' for name in options)
    body = []
    for r in rows:
        others = []
        if 'LOW_VENDOR_CONFIDENCE' in r.get('flags', []): others.append('Vendor needs confirmation')
        if 'DEFAULT_GL' in r.get('flags', []): others.append('GL account needs confirmation')
        day = '/'.join(reversed(r['date'].split('-')))
        body.append(f'''<tr data-id="{esc(r['provenance_hash'])}" data-cents="{r['amount_cents']}">
<td>{r['row_number']}</td><td>{esc(day)}</td>
<td>{esc(r['vendor_name'])}<small>{esc(r['merchant_original'])}</small></td>
<td class="amount">{money(r['amount_cents'])}</td>
<td>{esc(r.get('property_hint') or 'Not specified in the source file')}<small>{esc('; '.join(others))}</small></td>
<td><select class="property" aria-label="Property for row {r['row_number']}">{options_html}</select></td>
<td><input class="notes" aria-label="Notes for row {r['row_number']}" placeholder="Details or allocation"></td></tr>''')
    matched = plan.get('matched_existing_summary')
    exclusion_note = ''
    if matched:
        exclusion_note = (f"<p><b>Excluded from bulk import:</b> {int(matched['count'])} transactions "
                          f"totaling {esc(currency)} {money(matched['amount_cents'])} are already recorded "
                          "in AppFolio. These transactions are also excluded from the property assignment list below.</p>")
    template = '''<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unresolved Property Assignments</title>
<style>
body{font:15px system-ui,sans-serif;color:#172b3a;margin:32px auto;max-width:1400px;padding:0 20px;background:#f8fafc}
h1{font-size:28px;margin-bottom:8px}p{line-height:1.5}.summary{display:flex;gap:30px;background:#123d50;color:white;padding:20px;border-radius:8px;margin:20px 0}
.summary strong{font-size:25px;display:block}.tools{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin:18px 0}
button,input,select{font:inherit;padding:9px;border:1px solid #bccbd4;border-radius:4px}button{background:#123d50;color:white;cursor:pointer}
table{width:100%;border-collapse:collapse;background:white}th{text-align:left;background:#e5eef3;padding:12px;position:sticky;top:0}td{padding:10px;border-bottom:1px solid #dbe3e8;vertical-align:top}
small{display:block;color:#526575;font-size:12px;margin-top:5px}.amount{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
td select{width:260px;max-width:100%;background:#fffbe9}td input{width:150px;box-sizing:border-box;background:#fffbe9}#search{min-width:280px}#status{color:#526575}
@media print{body{margin:0;padding:0;background:white;font-size:10px}.tools,.instructions{display:none}th{position:static}thead{display:table-header-group}tr{break-inside:avoid}td,th{padding:6px}td input{width:110px;font-size:10px;border:0;border-bottom:1px solid #aaa}.summary{color:#172b3a;background:white;border:1px solid #aaa}small{font-size:9px}@page{size:A4 landscape;margin:12mm}}
</style>
<h1>Unresolved Property Assignments</h1>
<p>@@SOURCE@@ · @@CURRENCY@@ · Transactions with unresolved properties, including those with other review issues.</p>
<div class="summary"><div><strong>@@COUNT@@</strong>transactions awaiting property assignment</div><div><strong>@@CURRENCY@@ @@TOTAL@@</strong>total amount</div></div>
@@EXCLUSIONS@@
<p class="instructions">Select the correct property for each charge from the updated directory. If a charge belongs to multiple properties, describe the allocation in Notes. You may leave rows pending if a receipt or clarification is needed. Charges already assigned to Office are excluded.</p>
<p class="instructions">Your changes are kept only while this page is open. Click <b>Download responses</b> before closing it. Responses will be reviewed before any accounting updates; this report does not update the system.</p>
<div class="tools"><input id="search" placeholder="Search merchant, date or row" aria-label="Search transactions"><button id="download">Download responses</button><button id="print">Print / save PDF</button><span id="status"></span></div>
<table><thead><tr><th>AMEX row</th><th>Date (DD/MM/YYYY)</th><th>Vendor / original merchant</th><th>Amount @@CURRENCY@@</th><th>Available property hint</th><th>Correct property</th><th>Notes</th></tr></thead><tbody>@@ROWS@@</tbody></table>
<script>
const rows=Array.from(document.querySelectorAll('tbody tr'));
function update(){const q=document.querySelector('#search').value.toLocaleLowerCase();let shown=0;for(const row of rows){row.hidden=!(Array.from(row.cells).slice(0,5).map(cell=>cell.textContent).join(' ')+' '+row.querySelector('.property').value+' '+row.querySelector('.notes').value).toLocaleLowerCase().includes(q);if(!row.hidden)shown++;}document.querySelector('#status').textContent=shown+' of '+rows.length+' transactions shown';}
document.querySelector('#search').addEventListener('input',update);for(const row of rows){row.querySelector('.property').addEventListener('change',update);}update();
document.querySelector('#print').onclick=()=>window.print();
document.querySelector('#download').onclick=()=>{const responses=rows.map(row=>({provenance_hash:row.dataset.id,source_row:Number(row.cells[0].textContent),amount_cents:Number(row.dataset.cents),property:row.querySelector('.property').value,notes:row.querySelector('.notes').value}));const blob=new Blob([JSON.stringify({report_type:'property_assignment_response',responses},null,2)],{type:'application/json'});const url=URL.createObjectURL(blob);const a=document.createElement('a');a.href=url;a.download='property_responses.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
</script></html>'''
    substitutions = {'@@EXCLUSIONS@@':exclusion_note,'@@SOURCE@@':esc(source_name),'@@CURRENCY@@':esc(currency),
                     '@@COUNT@@':str(len(rows)),'@@TOTAL@@':money(total),'@@ROWS@@':''.join(body)}
    for key,value in substitutions.items(): template=template.replace(key,value)
    destination=Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text(template,encoding='utf-8')
    return dict(count=len(rows),amount_cents=total,path=str(destination.resolve()))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    print(json.dumps(build_unresolved_properties_report(json.loads(args.plan.read_text(encoding='utf-8')),args.output)))
