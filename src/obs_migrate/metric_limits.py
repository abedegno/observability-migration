"""Opt-in finite MSIDX4 bulk budgets; logical accounting, not an RSS limit."""
from dataclasses import dataclass, asdict, replace
import re


@dataclass(frozen=True)
class MetricLimits:
    name: str = 'explicit-msidx4-v1'
    values: int = 100_000
    cursor_bytes: int = 8 * 1024**2

    def __post_init__(self):
        if not isinstance(self.name, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', self.name):
            raise ValueError('metric resource profile requires a bounded ASCII name')
        for field, cap in (('values', 1_000_000), ('cursor_bytes', 8 * 1024**2)):
            value = getattr(self, field)
            if type(value) is not int or not 1 <= value <= cap:
                raise ValueError('metric resource ' + field + ' outside bounded profile limits')

    def describe(self):
        return {'metric_resource_schema': 1, **asdict(self),
                'numeric_access': 'incremental-xor-cursors',
                'memory_scope': 'block wire bytes plus 1024-byte allowance per cursor;not process RSS;other caps unchanged'}


LARGE_METRICS = replace(MetricLimits(), name='bounded-large-msidx4-v1', values=1_000_000)


def validate(limits):
    if limits is not None:
        if type(limits) is not MetricLimits:
            raise ValueError('metric limits require a MetricLimits profile')
        limits.__post_init__()
    return limits
