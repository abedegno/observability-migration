"""Read-only runtime capabilities; setup does not certify any input format."""
import importlib.metadata
import platform
import shutil
from . import __version__,decoder_source


def doctor():
    versions={}
    for name in ('pyarrow','zstandard','lz4','datafusion'):
        try:versions[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:versions[name]=None
    try:
        decoder_source.load();decoder={'status':'ready',**decoder_source.describe()}
    except (ValueError,OSError,ImportError) as error:decoder={'status':'unavailable','error':str(error)}
    try:
        import _native_provider as bridge
        native={'status':'available','bridge_revision':bridge.BRIDGE_REVISION,'event_time_clauses_revision':getattr(bridge,'EVENT_TIME_CLAUSES_REVISION',None),
            'datafusion_rust':bridge.DATAFUSION_RUST_VERSION,'arrow_rust':bridge.ARROW_RUST_VERSION}
        native['compatible']=versions['datafusion']=='55.0.0' and (native['bridge_revision'],native['datafusion_rust'],native['arrow_rust'])==('obs-native-ffi-3','55.2.0','59.3.0')
    except (ImportError,OSError,AttributeError) as error:native={'status':'unavailable','compatible':False,'error':str(error)}
    ready=decoder['status']=='ready' and all(versions[name] is not None for name in ('pyarrow','zstandard'));zstd=shutil.which('zstd') is not None
    return {'runtime_schema':1,'toolkit':__version__,'python':platform.python_version(),'platform':platform.platform(),
        'dependencies':versions,'decoder':decoder,'native_federation_bridge':native,
        'capabilities':{'physical_event_indexes':True,'event_journal_gzip_or_plain':ready,'event_journal_zstd':ready,'event_journal_lz4':ready and versions['lz4'] is not None,
            'zstd_cli_integrity_preflight_available':zstd,'zstd_cli_required':False,'zstd_integrity_backend':'bounded_standard_frames_and_library_EOF_checksums','upstream_rust_decoder_required':False,
            'eager_sql':versions['datafusion']=='55.0.0','direct_native_sql':native['compatible']},
        'scope':'environment readiness only;experimental per-profile source validation remains mandatory;no native SPL/search visibility certification',
        'licensing':'toolkit original code Apache-2.0;upstream source remains external;missing audited upstream licence notice is not resolved by setup'}
