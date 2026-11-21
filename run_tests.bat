@echo off
REM Full unit + integration + crash-recovery test suite.
python -m unittest discover -s tests -p "test_*.py" -v
