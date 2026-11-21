# Local wheelhouse

All installs use `pip --no-index --find-links wheelhouse`; the application
never downloads packages at install or run time.

The checked-in wheels in this workspace target the runtime used for the
recorded verification: Linux x86-64 / CPython 3.14.  For native Windows,
populate the same directory with a matching `numpy` and `Pillow` wheel for the
selected CPython version (the package source itself supports Python 3.10+),
then run `scripts/setup_offline.ps1`.

