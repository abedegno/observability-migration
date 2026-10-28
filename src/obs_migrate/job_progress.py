"""Bounded, payload-free stage observations for the existing offline job workflow."""
from contextlib import contextmanager
import re
from time import perf_counter


class JobProgress:
    def __init__(self, callback, operation, plan=None):
        if callback is not None and not callable(callback):
            raise ValueError('progress must be a callable or None')
        self.callback, self.operation, self.plan = callback, operation, plan
        if callback is not None and plan is not None:
            from .index_job import identity, manifest
            try:
                valid = (1 <= len(plan.sources) <= 256 and
                         identity(manifest(plan)) == plan.selection_sha256)
            except (AttributeError, TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError('progress requires a validated job plan')

    def _emit(self, stage, status, member, number, duration=None):
        record = {'type':'job_progress', 'progress_schema':1,
                  'operation':self.operation, 'stage':stage, 'status':status}
        if duration is not None:
            record['duration_seconds'] = round(max(0.0, duration), 6)
        if self.plan is not None:
            record['members_total'] = len(self.plan.sources)
        if member is not None:
            if member not in self.plan.sources or not re.fullmatch('[0-9a-f]{64}', member.source_key):
                raise ValueError('progress requires a validated member')
            record.update(source_key=member.source_key, member_number=number)
            if re.fullmatch('[a-z][a-z0-9_-]{0,62}', member.input_id):
                record['input_id'] = member.input_id
            # Display only bounded ASCII basename/receipt identifiers. Other
            # valid selections remain identifiable by their opaque source key.
            if (member.selector not in ('.','..') and
                    re.fullmatch('[A-Za-z0-9_.~:-]{1,1024}', member.selector)):
                record['selector'] = member.selector
        self.callback(record)

    @contextmanager
    def stage(self, stage, member=None, number=None):
        if self.callback is None:
            yield
            return
        started = perf_counter()
        self._emit(stage, 'started', member, number)
        try:
            yield
        except BaseException as error:
            # Preserve the original failure/cancellation if reporting also fails.
            # No exception text or dynamic exception-class names enter the record.
            try:
                status = 'cancelled' if isinstance(error, KeyboardInterrupt) else 'failed'
                self._emit(stage, status, member, number, perf_counter()-started)
            except BaseException:
                pass
            raise
        else:
            self._emit(stage, 'completed', member, number, perf_counter()-started)
