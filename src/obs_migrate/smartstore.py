"""Observed SmartStore receipts over an acquired, hash-inventoried object set.

No network client, cache eviction, bucket-ownership inference or vendor hash
algorithm is implemented here. The observer's objects.json is a toolkit
acquisition inventory; receipt.json and every native payload remain original.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tempfile

from .capture import _read
from .source_identity import normalize_sha256, require_sha256

MAX_BYTES = 256 * 1024**2
MAX_INVENTORY = 2 * 1024**2
MAX_RECEIPT = 1024**2
MAX_OBJECTS = 4096
GUID = re.compile(r'[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}\Z')


def _sha(data):return hashlib.sha256(data).hexdigest()
def _copy(value):return json.loads(json.dumps(value))


def _json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:raise ValueError('duplicate JSON key in SmartStore acquisition')
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs)


def _relative(value):
    if (type(value) is not str or not value or len(value.encode('utf8')) > 1024
            or '\\' in value or '\x00' in value or value.startswith('/')
            or any(part in ('', '.', '..') for part in value.split('/'))):
        raise ValueError('unsafe or unsupported SmartStore object/file path')
    return value


class AcquiredObjects:
    """Pin all observed objects before accepting any bucket or empty query.

    Inventory SHA is only authentication when supplied from a trusted independent
    acquisition. ETags and native user_data.content_hash are retained opaque.
    """
    def __init__(self, path, *, offline_snapshot=False, expected_inventory_sha256=None):
        if offline_snapshot is not True:raise ValueError('SmartStore acquisition requires offline_snapshot assertion')
        expected_inventory_sha256 = normalize_sha256(expected_inventory_sha256)
        root = Path(path).absolute()
        if any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
            raise ValueError('SmartStore acquisition requires a regular local root without symlinks')
        raw = _read(root/'objects.json', MAX_INVENTORY)
        self.inventory_sha256 = _sha(raw)
        require_sha256(self.inventory_sha256, expected_inventory_sha256, 'SmartStore acquisition inventory')
        inventory = _json(raw)
        if type(inventory) is not list or not 1 <= len(inventory) <= MAX_OBJECTS:
            raise ValueError('SmartStore object inventory count/shape unsupported')
        self._objects = {};self._inventory = {};local_names = set();total = 0
        for item in inventory:
            if type(item) is not dict:raise ValueError('SmartStore object inventory entry must be an object')
            key = _relative(item.get('key'));name = _relative(item.get('file'))
            size = item.get('size');sha = normalize_sha256(item.get('sha256'))
            if sha is None or type(size) is not int or size < 0:
                raise ValueError('SmartStore inventory requires exact size and SHA256')
            if key in self._objects or name in local_names:raise ValueError('duplicate SmartStore object key/local file')
            local_names.add(name);total += size
            if total > MAX_BYTES:raise ValueError('SmartStore acquired object byte budget exceeded')
            file = root/name
            if any(p.is_symlink() for p in (file, *file.parents)):
                raise ValueError('SmartStore acquisition contains a symlink')
            data = _read(file, size, zero=True)
            if len(data) != size or _sha(data) != sha:raise ValueError('SmartStore acquired object size/SHA256 mismatch')
            self._objects[key] = data;self._inventory[key] = dict(item)
        if _read(root/'objects.json', MAX_INVENTORY) != raw:
            raise ValueError('SmartStore inventory changed during acquisition pinning')
        for key, item in self._inventory.items():
            file = root/item['file']
            if any(p.is_symlink() for p in (file, *file.parents)) or _read(file, item['size'], zero=True) != self._objects[key]:
                raise ValueError('SmartStore acquired source changed during pinning')
        self._buckets = {};assigned = set()
        for key in sorted(self._objects):
            if not key.endswith('/receipt.json'):continue
            if len(self._objects[key]) > MAX_RECEIPT:raise ValueError('SmartStore receipt byte limit exceeded')
            receipt = _json(self._objects[key])
            if type(receipt) is not dict or set(receipt) != {'objects','manifest','user_data'}:
                raise ValueError('unverified SmartStore receipt schema')
            manifest = receipt['manifest'];user = receipt['user_data'];objects = receipt['objects']
            if type(manifest) is not dict or type(user) is not dict or type(objects) is not list or not 1 <= len(objects) <= MAX_OBJECTS:
                raise ValueError('unverified SmartStore receipt fields')
            identifier = manifest.get('id');guid = user.get('uploader_guid')
            if type(identifier) is not str or len(identifier.split('~')) != 3 or type(guid) is not str or not GUID.fullmatch(guid):
                raise ValueError('unverified SmartStore bucket/uploader identity')
            index, local_id, origin_guid = identifier.split('~')
            if not index or not local_id.isdecimal() or not GUID.fullmatch(origin_guid):
                raise ValueError('unverified SmartStore bucket origin identity')
            bucket_name = _relative(manifest.get('path'))
            if '/' in bucket_name or not bucket_name.startswith('db_') or not bucket_name.endswith('_'+origin_guid):
                raise ValueError('unverified SmartStore native bucket path')
            prefix = key.rsplit('/', 1)[0]
            uploader = 'guidSplunk-'+guid
            distinct=origin_guid!=guid
            if distinct and not bucket_name.endswith('_'+local_id+'_'+origin_guid):
                raise ValueError('distinct-uploader bucket path/local id mismatch')
            if prefix.rsplit('/',1)[-1] != local_id+'~'+origin_guid:
                raise ValueError('SmartStore receipt key/bucket identity mismatch')
            members = {};member_keys = set();expansions = []
            for item in objects:
                if type(item) is dict and set(item)=={'name','size','expand'}:
                    if (item['name']!='./'+uploader+'/rawdata/deletes/' or
                            type(item['size']) is not int or item['size']!=-1 or item['expand'] is not True):
                        raise ValueError('unverified SmartStore receipt directory expansion')
                    if expansions:raise ValueError('duplicate SmartStore delete directory expansion')
                    directory=prefix+'/'+uploader+'/rawdata/deletes/'
                    expanded=sorted(k for k in self._objects if k.startswith(directory))
                    if not 1<=len(expanded)<=256:raise ValueError('SmartStore delete expansion count unsupported')
                    for object_key in expanded:
                        leaf=object_key[len(directory):]
                        if not re.fullmatch(r'[0-9a-f]{32}\.csv\.gz',leaf):
                            raise ValueError('unverified SmartStore delete expansion member')
                        relative='rawdata/deletes/'+leaf
                        if relative in members:raise ValueError('duplicate SmartStore expanded object')
                        members[relative]=object_key;member_keys.add(object_key)
                    expansions.append({'relative_prefix':'rawdata/deletes/','object_prefix':directory,
                                       'object_keys':expanded,'semantics':'observed_stopped_inventory_expansion'})
                    continue
                if type(item) is not dict or set(item) != {'name','size'} or type(item['size']) is not int or item['size'] < 0:
                    raise ValueError('unverified SmartStore receipt object shape')
                name = item['name']
                if type(name) is not str or not name.startswith('./'+uploader+'/'):
                    raise ValueError('SmartStore receipt object outside observed uploader prefix')
                relative = _relative(name[len('./'+uploader+'/'):])
                object_key = prefix+'/'+_relative(name[2:])
                if relative in members or relative in ('receipt.json','.obs-smartstore.json'):
                    raise ValueError('SmartStore receipt object name collision')
                if object_key not in self._objects or len(self._objects[object_key]) != item['size']:
                    raise ValueError('SmartStore receipt references missing or wrong-size object')
                members[relative] = object_key;member_keys.add(object_key)
            # The observed profile has one generation/uploader and a closed set
            # of sibling objects. Never silently omit unlisted files/deletes.
            siblings = {k for k in self._objects if k.startswith(prefix+'/')}
            if siblings != member_keys | {key}:
                raise ValueError('SmartStore uploader has unlisted/extra objects')
            if identifier in self._buckets:raise ValueError('multiple SmartStore generations for one bucket are ambiguous')
            if distinct and any(name.startswith('rawdata/deletes/') for name in members):
                raise ValueError('unqualified distinct-uploader deletion context')
            journal_names = [n for n in members if n in ('rawdata/journal.gz','rawdata/journal.zst')]
            if len(journal_names) != 1:raise ValueError('SmartStore receipt requires exactly one observed journal')
            self._buckets[identifier] = {'id': identifier, 'index': index, 'bucket_name': bucket_name,
                'receipt_key': key, 'receipt_sha256': _sha(self._objects[key]),
                'manifest': manifest, 'native_user_data': user, 'native_content_hash_verified': False,
                **({'expanded_prefixes':expansions} if expansions else {}),
                'members': members, 'journal_key': members[journal_names[0]],
                'selection': 'explicit_physical_copy_only', 'search_visibility': 'UNVERIFIED',
                **({'copy_identity':{'origin_guid':origin_guid,'uploader_guid':guid,
                    'relation':'observed_distinct_origin_uploader','ownership':'UNVERIFIED'}} if distinct else {})}
            assigned.update(siblings)
        if not self._buckets:raise ValueError('no supported native SmartStore receipts in acquisition')
        self._report = {'format': 'obs-acquired-smartstore-1', 'inventory_sha256': self.inventory_sha256,
            'objects': len(self._objects), 'bytes': total, 'buckets': list(self._buckets.values()),
            'unassigned_objects': sorted(set(self._objects)-assigned),
            'acquisition_consistency': 'caller_asserted_stopped_producer',
            'source_completeness': 'receipt_inventory_agreement_only', 'search_visibility': 'UNVERIFIED'}

    def report(self):return _copy(self._report)

    @contextmanager
    def materialize(self, bucket_id):
        """Temporary exact native file view; never write to acquisition sources."""
        if bucket_id not in self._buckets:raise ValueError('explicit SmartStore bucket id absent from acquisition')
        bucket = self._buckets[bucket_id]
        with tempfile.TemporaryDirectory(prefix='obs-smartstore-') as temporary:
            root = Path(temporary)/bucket['bucket_name'];root.mkdir()
            for name, key in bucket['members'].items():
                target = root/PurePosixPath(name);target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(self._objects[key])
            (root/'receipt.json').write_bytes(self._objects[bucket['receipt_key']])
            provenance = {'format': 'obs-smartstore-materialization-1', 'inventory_sha256': self.inventory_sha256,
                          **bucket, 'object_hashes': {key:_sha(self._objects[key]) for key in bucket['members'].values()}}
            (root/'.obs-smartstore.json').write_text(json.dumps(provenance,sort_keys=True)+'\n')
            yield root

    @contextmanager
    def event_bucket(self, bucket_id, *, experimental=False, backend='arrow', visibility='physical', limits=None):
        """Reuse the existing complete physical reader and its Arrow contracts."""
        from .event_bucket import EventBucket
        if bucket_id not in self._buckets:raise ValueError('explicit SmartStore bucket id absent from acquisition')
        bucket = self._buckets[bucket_id]
        masks=None
        if visibility=='undeleted' and bucket.get('expanded_prefixes'):
            masks={name:{'bytes':len(self._objects[key]),'sha256':_sha(self._objects[key])}
                   for name,key in bucket['members'].items() if name.startswith('rawdata/deletes/')}
        with self.materialize(bucket_id) as root:
            with EventBucket(root,experimental=experimental,offline_snapshot=True,backend=backend,
                             visibility=visibility,limits=limits,
                             _smartstore_mask_delta=masks,
                             index=bucket['index'],bucket_id=bucket_id,
                             object_uri='smartstore-object-key:'+bucket['journal_key']) as snapshot:
                yield snapshot
