"""Wall-clock seam: engine code reads clock.now(); tests replace it, and monotonic is exported here too."""

from time import monotonic
from time import time as now

__all__ = ["monotonic", "now"]
