"""Shared release metadata and project location for PBS maintenance."""
from pathlib import Path

__version__ = "0.0.7"
# Keep config/logs beside the original launcher, not inside this package.
SCRIPT_DIR = Path(__file__).resolve().parent.parent
