"""Optional caller-provided byte identity; trust originates outside this reader."""
import re


class SourceIdentityError(ValueError):pass


def normalize_sha256(value):
    if value is None:return None
    if type(value) is not str or not re.fullmatch(r'[0-9a-fA-F]{64}',value):
        raise SourceIdentityError('expected SHA-256 requires exactly 64 hexadecimal characters')
    return value.lower()


def require_sha256(actual,expected,scope):
    expected=normalize_sha256(expected)
    if expected is not None and actual!=expected:raise SourceIdentityError(scope+' SHA-256 differs from expected identity')
