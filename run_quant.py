from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str((Path(__file__).parent / "src").resolve()))

from quant_research.main import main


if __name__ == "__main__":
    main()
