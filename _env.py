"""
Shared .env loader — module unique pour tous les scripts.
Placé à la racine pour être trouvé facilement par sys.path.

Usage (en haut de chaque script):
    import _env  # noqa: F401  (cet import charge le .env dans os.environ)

Plus simple que `from common.env import load_env` qui nécessite que le path
du projet soit déjà connu de Python.
"""
import os
import sys
from pathlib import Path


def _find_env_file() -> Path:
    """Cherche un .env en remontant depuis le CWD ou depuis ce fichier."""
    # 1) CWD
    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        return cwd_env
    # 2) Remonter depuis ce fichier
    for parent in Path(__file__).parents:
        candidate = parent / ".env"
        if candidate.exists():
            return candidate
    return None


_ENV_PATH = _find_env_file()

if _ENV_PATH is not None:
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV_PATH, override=False)
    except ImportError:
        # Fallback : parsing manuel (gère quotes et commentaires)
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1]
            os.environ.setdefault(k, v)
