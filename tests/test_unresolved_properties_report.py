from scripts.output.unresolved_properties_report import build_unresolved_properties_report


def test_all_property_flags_count_even_other_main_reason(tmp_path):
    base=dict(date='2026-07-01',row_number=2,vendor_name='Vendor',merchant_original='<script>bad</script>',property_hint='',amount_cents=1234,property_resolved='REVISAR PROP: X')
    rows=[dict(base,provenance_hash='a',flags=['UNRESOLVED_PROPERTY','LOW_VENDOR_CONFIDENCE']),dict(base,provenance_hash='b',row_number=3,amount_cents=-100,flags=['UNRESOLVED_PROPERTY']),dict(base,provenance_hash='c',property_resolved='Office',flags=[],amount_cents=11900)]
    result=build_unresolved_properties_report({'details':rows},tmp_path/'report.html')
    assert result['count']==2 and result['amount_cents']==1134
    text=(tmp_path/'report.html').read_text(encoding='utf-8')
    assert '<script>bad</script>' not in text and '&lt;script&gt;bad' in text
    assert 'data-id="c"' not in text
    assert 'Correct property' in text and 'Download responses' in text
