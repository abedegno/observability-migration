# Optional federation

The Python readers expose Arrow batches for downstream engines. Install the
optional Python DataFusion dependency with `pip install '.[federation]'`.
Providers operate on validated immutable physical sources; SQL engine residuals
remain responsible for full predicate evaluation. This is not SPL or general
native indexed-search equivalence.

The original Rust/PyO3 adapter lives in [native/obs-native](../native/obs-native).
It bridges existing Python readers to DataFusion using Arrow interfaces; it does
not independently implement a proprietary bucket decoder. Optional projection
and necessary-condition filters retain engine residuals and lifecycle checks.

Build in an activated virtual environment with Rust/Cargo installed:

```sh
python -m pip install -e '.[federation]'
python scripts/build_native_provider.py --release
```

`Cargo.lock` fixes the Rust dependency graph. This is an interpreter-specific
experimental extension, not a production binary wheel. The portable journal
path does not require it. Source validation and initial scans are not removed
by lazy registration, and decompressed addressing is not compressed random seek.
