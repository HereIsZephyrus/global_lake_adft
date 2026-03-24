"""Development convenience entry point.

Use the ``hydrofetch`` console script (installed via ``pip install -e .``)
for normal usage.  This file allows running the CLI directly during
development::

    python main.py era5 --help
"""

from hydrofetch.cli import main

if __name__ == "__main__":
    main()
