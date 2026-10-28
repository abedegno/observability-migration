import pytest


@pytest.fixture
def row():
    return dict(schema_version=3, is_deleted=None, bucket_disabled=None, bucket_context_json="{}", raw=b"\xff\x00hello\n", display_text="�\x00hello\n", event_time=None,
                index_time=None, host=b"host\xff", source=b"", sourcetype=None, index=None,
                bucket_id=None, object_uri=None, source_sha256="a"*64, record_ordinal=0,
                decompressed_offset=7, record_wire=b"opaque test bytes, not a journal",
                metadata_json='[{"key":"/w==","representation":0,"operand":"1","value_base64":"AA==","wire":"AA=="}]',
                decoder_state_json='{"time_sub_seconds_shifted":123456789}',
                decoder_warnings=["test_contract_only"], decoder="canonical-test-1")
