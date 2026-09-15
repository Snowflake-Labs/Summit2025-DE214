import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

if "psutil" not in sys.modules:
    sys.modules["psutil"] = types.SimpleNamespace(Process=lambda: None)
