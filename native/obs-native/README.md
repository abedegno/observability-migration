# Experimental DataFusion adapter

Original Apache-2.0 Rust/PyO3 source bridging the existing Python physical readers
to DataFusion through Arrow interfaces. It does not reimplement Splunk parsing.

From the repository root, in an activated virtual environment:

```sh
python -m pip install -e '.[federation]'
python scripts/build_native_provider.py --release
```

Requires Rust/Cargo and the pinned `Cargo.lock` graph. Use `--offline` only after
fetching dependencies. The default build is interpreter-specific. No production
native binary is bundled with the Python package. Filters remain engine
residuals, source validation is mandatory and general indexed search is unsupported.
See [federation](../../docs/federation.md) for the scope.
