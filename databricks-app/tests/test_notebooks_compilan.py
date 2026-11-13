"""Todos los .py del proyecto deben ser Python válido (por ejemplo, sin errores de indentación)."""

from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def test_todos_los_py_compilan():
    errores = []
    for archivo in sorted(RAIZ.rglob("*.py")):
        if ".venv" in archivo.parts:
            continue
        try:
            compile(archivo.read_text(encoding="utf-8"), str(archivo), "exec")
        except SyntaxError as e:
            errores.append(f"{archivo.relative_to(RAIZ)}:{e.lineno}: {e.msg}")
    assert not errores, "Archivos con errores de sintaxis:\n" + "\n".join(errores)
