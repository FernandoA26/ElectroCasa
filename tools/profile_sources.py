"""Perfil reproducible de los archivos del caso ElectroCasa."""
import csv
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def rows_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def empty(value):
    return value is None or str(value).strip() == ""


def num(value):
    try:
        return float(str(value).replace("S/", "").strip())
    except (ValueError, TypeError):
        return None


def main():
    sources = {
        "ventas": (rows_csv(DATA / "ventas_sucursales.csv"), "venta_id"),
        "productos": (json.loads((DATA / "catalogo_productos.json").read_text()), "producto_id"),
        "empleados": (rows_csv(DATA / "empleados_rrhh.csv"), "id_empleado"),
        "resenas": (json.loads((DATA / "resenas_clientes.json").read_text()), "resena_id"),
        "devoluciones": (rows_csv(DATA / "devoluciones.csv"), "devolucion_id"),
    }
    result = {}
    for name, (rows, key) in sources.items():
        ids = Counter(str(row.get(key)) for row in rows)
        result[name] = {
            "archivo": {"ventas": "ventas_sucursales.csv", "productos": "catalogo_productos.json",
                        "empleados": "empleados_rrhh.csv", "resenas": "resenas_clientes.json",
                        "devoluciones": "devoluciones.csv"}[name],
            "filas": len(rows),
            "columnas": list(rows[0]),
            "ids_distintos": len(ids),
            "filas_con_id_repetido": sum(max(0, count - 1) for count in ids.values()),
            "nulos_o_vacios": {field: sum(empty(row.get(field)) for row in rows)
                                for field in rows[0] if any(empty(row.get(field)) for row in rows)},
        }
    result["ventas"]["monto_no_positivo"] = sum(num(r["monto_total"]) is not None and num(r["monto_total"]) <= 0 for r in sources["ventas"][0])
    result["productos"]["precio_no_positivo_o_no_numerico"] = sum(num(r["precio_lista"]) is None or num(r["precio_lista"]) <= 0 for r in sources["productos"][0])
    result["resenas"]["calificacion_fuera_de_1_a_5"] = sum(num(r["calificacion"]) is None or not 1 <= num(r["calificacion"]) <= 5 for r in sources["resenas"][0])
    result["devoluciones"]["reembolso_negativo"] = sum(num(r["monto_reembolso"]) is not None and num(r["monto_reembolso"]) < 0 for r in sources["devoluciones"][0])
    sql = (DATA / "tracking_envios_azure_sql.sql").read_text(encoding="utf-8")
    result["tracking_script"] = {
        "archivo": "tracking_envios_azure_sql.sql",
        "tabla": "dbo.TrackingEnvios",
        "filas_insert_sinteticas": sum(line.lstrip().startswith("('TRK") for line in sql.splitlines()),
        "nota": "Conteo de sentencias de inserción del script de referencia",
    }
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
