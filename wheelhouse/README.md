# Offline wheelhouse

Put the locally cached Windows-compatible wheels here before running
`scripts/setup_windows.ps1`.  No package is downloaded from the network by the
setup script.

On a machine that already has an approved local wheel cache, collect the files
without reaching the public Internet, for example:

```powershell
python -m pip download --only-binary=:all: --no-index `
  --find-links C:\approved-wheel-cache `
  -d wheelhouse -r requirements.txt
```

The target machine only needs this directory plus a Python 3.11+ installer.
