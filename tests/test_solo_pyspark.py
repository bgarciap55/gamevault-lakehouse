"""Regla del curso: solo PySpark, nada de Spark SQL ni SQL en el código."""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"

PROHIBIDO = [
    re.compile(r"\bspark\.sql\s*\("),
    re.compile(r"\.sql\s*\(\s*f?[\"']"),
    re.compile(r"^#\s*MAGIC\s+%sql", re.MULTILINE),
    re.compile(r"\bselectExpr\s*\("),
    re.compile(r"\bF\.expr\s*\("),
]


def test_sin_sql_en_src():
    hallazgos = []
    for archivo in SRC.rglob("*.py"):
        texto = archivo.read_text(encoding="utf-8")
        for patron in PROHIBIDO:
            for m in patron.finditer(texto):
                linea = texto.count("\n", 0, m.start()) + 1
                hallazgos.append(f"{archivo.relative_to(SRC)}:{linea}: {m.group(0)}")
    assert not hallazgos, "SQL encontrado:\n" + "\n".join(hallazgos)
