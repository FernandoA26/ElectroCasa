"""Comprueba estructura y sintaxis del proyecto."""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    'databricks.yml', 'resources/electrocasa_pipeline.yml',
    'resources/electrocasa_job.yml', 'src/electrocasa/transformations/pipeline.py',
    'notebooks/00_setup.ipynb', 'notebooks/01_ingest_snapshots.ipynb',
    'notebooks/02_verify.ipynb', 'notebooks/03_governance.ipynb',
    'notebooks/04_create_connection.ipynb', 'README.md',
]
for item in REQUIRED:
    assert (ROOT / item).is_file(), f'Falta {item}'
for script in ROOT.rglob('*.py'):
    ast.parse(script.read_text(encoding='utf-8'), filename=str(script))
for notebook in (ROOT / 'notebooks').glob('*.ipynb'):
    obj = json.loads(notebook.read_text(encoding='utf-8'))
    assert obj['nbformat'] == 4
    for cell in obj['cells']:
        if cell['cell_type'] == 'code':
            ast.parse(''.join(cell['source']), filename=str(notebook))
try:
    import yaml
except ImportError:
    print('YAML: lectura de texto; instalar PyYAML para validación local del parser')
else:
    for config in ROOT.rglob('*.yml'):
        assert isinstance(yaml.safe_load(config.read_text(encoding='utf-8')), dict)
    print('YAML: sintaxis local legible por PyYAML')
print(f'Estructura: {len(REQUIRED)} archivos requeridos presentes')
print('Python y notebooks: sintaxis válida')
