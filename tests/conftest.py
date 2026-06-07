"""Configuração global dos testes — garante compatibilidade PySpark no Windows."""

import os
import platform
import sys

# ── Configura HADOOP_HOME no Windows antes de qualquer import do PySpark ──
if platform.system() == "Windows":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "DTM", "DTM"))
    from main import _setup_windows

    _setup_windows()
