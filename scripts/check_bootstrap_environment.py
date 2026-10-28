"""Read-only check before reusing a bootstrap virtual environment."""
import importlib.metadata
import sys


def main():
    try:
        importlib.metadata.distribution('obs-migrate')
    except importlib.metadata.PackageNotFoundError:
        return 0
    print('The legacy obs-migrate distribution is installed in this environment. '
          'Use a fresh .venv for observability-migration-toolkit; the distributions '
          'share the obs_migrate Python namespace. Keep the old environment '
          'separately. No packages have been changed.',file=sys.stderr)
    return 1


if __name__=='__main__':
    raise SystemExit(main())
