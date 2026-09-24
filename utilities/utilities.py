
from pathlib import Path

def find_project_root() -> Path:
    path = Path(__file__).resolve()

    for parent in [path, *path.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise RuntimeError("Could not find project root")

PROJECT_ROOT = find_project_root()