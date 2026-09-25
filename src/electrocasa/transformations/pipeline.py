"""Lakeflow Spark Declarative Pipeline: ElectroCasa, seis fuentes, tres capas."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG = spark.conf.get("project.catalog")
LANDING = spark.conf.get("project.landing")
STATE = spark.conf.get("project.state")


def table(layer, name):
    return f"{CATALOG}.{layer}.{name}"


def files(name, fmt):
    options = {"cloudFiles.format": fmt, "cloudFiles.schemaEvolutionMode": "addNewColumns",
               "cloudFiles.schemaLocation": f"{STATE}/schemas/{name}"}
    if fmt == "csv":
        options.update({"header": "true", "inferColumnTypes": "false"})
    if fmt == "json":
        options.update({"multiLine": "true"})
    return (spark.readStream.format("cloudFiles").options(**options)
            .load(f"{LANDING}/{name}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_batch_id", F.col("_metadata.file_modification_time").cast("string")))


@dp.table(name=table("bronze", "ventas"), comment="CSV raw; Auto Loader incremental")
def bronze_ventas():
    return files("ventas", "csv")


@dp.table(name=table("bronze", "resenas"), comment="JSON raw; Auto Loader incremental")
def bronze_resenas():
    return files("resenas", "json")


@dp.table(name=table("bronze", "devoluciones"), comment="CSV raw; Auto Loader incremental")
def bronze_devoluciones():
    return files("devoluciones", "csv")


def latest(df, key, order_column="_ingested_at"):
    # Orden estable para empates; no convierte 2 eventos RR. HH. distintos en duplicados.
    w = Window.partitionBy(key).orderBy(F.col(order_column).desc_nulls_last(),
                                        F.col("_source_file").desc_nulls_last(),
                                        F.sha2(F.to_json(F.struct(*df.columns)), 256).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def sales_clean():
    df = spark.read.table(table("bronze", "ventas"))
    return (df.withColumn("monto_total", F.col("monto_total").cast("decimal(18,2)"))
            .withColumn("cantidad", F.col("cantidad").cast("int"))
            .withColumn("fecha_venta", F.coalesce(F.to_date("fecha_venta", "yyyy-MM-dd"),
                                                   F.to_date("fecha_venta", "dd/MM/yyyy")))
            .withColumn("metodo_pago", F.when(F.upper(F.trim("metodo_pago")).isin("TC", "TARJETA"), "tarjeta")
                        .when(F.upper(F.trim("metodo_pago")).isin("EFV", "EFECTIVO"), "efectivo")
                        .otherwise(F.lower(F.trim("metodo_pago")))))


@dp.materialized_view(name=table("silver", "ventas"))
@dp.expect_or_drop("venta_valida", "venta_id IS NOT NULL AND sucursal_id IS NOT NULL AND producto_id IS NOT NULL AND monto_total > 0 AND cantidad > 0 AND fecha_venta IS NOT NULL")
def silver_ventas():
    return latest(sales_clean(), "venta_id")


def products_clean():
    return (spark.read.table(table("bronze", "productos"))
            .withColumn("precio_lista", F.regexp_replace(F.col("precio_lista").cast("string"), r"[^0-9.\-]", "").cast("decimal(18,2)"))
            .withColumn("categoria", F.lower(F.trim("categoria")))
            .withColumn("categoria", F.translate("categoria", "áéíóúÁÉÍÓÚ", "aeiouAEIOU")))


@dp.materialized_view(name=table("silver", "productos"))
@dp.expect_or_drop("precio_valido", "producto_id IS NOT NULL AND precio_lista > 0")
def silver_productos():
    return latest(products_clean(), "producto_id")


def reviews_clean():
    return (spark.read.table(table("bronze", "resenas"))
            .withColumn("calificacion", F.col("calificacion").cast("int"))
            .withColumn("fecha_resena", F.to_date("fecha_resena")))


@dp.materialized_view(name=table("silver", "resenas"))
@dp.expect_or_drop("calificacion_valida", "resena_id IS NOT NULL AND producto_id IS NOT NULL AND calificacion BETWEEN 1 AND 5")
def silver_resenas():
    return latest(reviews_clean(), "resena_id")


def returns_clean():
    return (spark.read.table(table("bronze", "devoluciones"))
            .withColumn("monto_reembolso", F.col("monto_reembolso").cast("decimal(18,2)"))
            .withColumn("fecha_devolucion", F.to_date("fecha_devolucion")))


@dp.materialized_view(name=table("silver", "devoluciones"))
@dp.expect_or_drop("reembolso_valido", "devolucion_id IS NOT NULL AND producto_id IS NOT NULL AND monto_reembolso >= 0")
def silver_devoluciones():
    return latest(returns_clean(), "devolucion_id")


def staff_clean():
    return (spark.read.table(table("bronze", "empleados"))
            .withColumn("fecha_evento", F.to_date("fecha_evento"))
            .withColumn("salario", F.col("salario").cast("decimal(18,2)"))
            .withColumn("tipo_evento", F.lower(F.trim("tipo_evento"))))


@dp.materialized_view(name=table("silver", "empleados_eventos"))
@dp.expect_or_drop("empleado_identificable", "id_empleado IS NOT NULL AND dni IS NOT NULL AND fecha_evento IS NOT NULL AND tipo_evento IN ('alta','transferencia','cambio_salario','baja')")
def silver_empleados_eventos():
    df = staff_clean()
    # La clave es id_empleado: los DNI repetidos entre empleados son anomalías a investigar.
    return latest(df.withColumn("_event_key", F.sha2(F.concat_ws("|", "id_empleado", F.col("fecha_evento").cast("string"), "tipo_evento", "sucursal_id", F.col("salario").cast("string")), 256)), "_event_key").drop("_event_key")


@dp.materialized_view(name=table("silver", "empleados_hist"), comment="SCD tipo 2 por evento; vigencia [inicio, fin)")
def silver_empleados_hist():
    df = spark.read.table(table("silver", "empleados_eventos"))
    w = Window.partitionBy("id_empleado").orderBy("fecha_evento", "tipo_evento", "_source_file")
    return (df.withColumn("vigente_desde", F.col("fecha_evento"))
            .withColumn("vigente_hasta", F.lead("fecha_evento").over(w))
            .withColumn("es_actual", F.col("vigente_hasta").isNull())
            .withColumn("esta_activo", F.col("es_actual") & (F.col("tipo_evento") != "baja")))


def tracking_clean():
    df = spark.read.table(table("bronze", "tracking"))
    state = F.lower(F.trim(F.col("estado_entrega")))
    return (df.withColumn("estado_entrega", F.when(state.isin("en camino", "en_camino", "en_transito"), "en_transito")
                .when(state.isin("entregado", "pendiente", "devuelto"), state)
                .otherwise("desconocido"))
            .withColumn("courier", F.initcap(F.lower(F.trim("courier")))))


@dp.materialized_view(name=table("silver", "tracking"))
@dp.expect_or_drop("estado_valido", "tracking_id IS NOT NULL AND estado_entrega IN ('entregado','en_transito','pendiente','devuelto')")
def silver_tracking():
    return latest(tracking_clean(), "tracking_id", "fecha_actualizacion")


@dp.materialized_view(name=table("silver", "cuarentena"), comment="Acceso solo Ingeniería; contiene payload original con posibles datos personales")
def cuarentena():
    checks = [
        ("ventas", sales_clean(), "venta_id IS NULL OR sucursal_id IS NULL OR producto_id IS NULL OR monto_total IS NULL OR monto_total <= 0 OR cantidad IS NULL OR cantidad <= 0 OR fecha_venta IS NULL", "venta_invalida"),
        ("productos", products_clean(), "producto_id IS NULL OR precio_lista IS NULL OR precio_lista <= 0", "precio_invalido"),
        ("empleados", staff_clean(), "id_empleado IS NULL OR dni IS NULL OR fecha_evento IS NULL OR tipo_evento NOT IN ('alta','transferencia','cambio_salario','baja')", "empleado_invalido"),
        ("resenas", reviews_clean(), "resena_id IS NULL OR producto_id IS NULL OR calificacion IS NULL OR calificacion NOT BETWEEN 1 AND 5", "resena_invalida"),
        ("devoluciones", returns_clean(), "devolucion_id IS NULL OR producto_id IS NULL OR monto_reembolso IS NULL OR monto_reembolso < 0", "reembolso_invalido"),
        ("tracking", tracking_clean(), "tracking_id IS NULL OR estado_entrega = 'desconocido'", "estado_invalido"),
    ]
    parts = []
    for source, df, condition, reason in checks:
        parts.append(df.filter(condition).select(F.lit(source).alias("fuente"), F.lit(reason).alias("motivo"),
            "_source_file", "_ingested_at", "_batch_id", F.to_json(F.struct(*df.columns)).alias("registro_original")))
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result


@dp.materialized_view(name=table("gold", "ventas_sucursal_mes"))
def gold_ventas_sucursal_mes():
    return (spark.read.table(table("silver", "ventas"))
            .groupBy("sucursal_id", F.date_trunc("month", "fecha_venta").cast("date").alias("mes"))
            .agg(F.count("venta_id").alias("num_ventas"), F.sum("monto_total").alias("ventas_total"),
                 F.avg("monto_total").alias("ticket_promedio")))


@dp.materialized_view(name=table("gold", "productos_desempeno"))
def gold_productos_desempeno():
    v = spark.read.table(table("silver", "ventas")).groupBy("producto_id").agg(F.sum("cantidad").alias("unidades"), F.sum("monto_total").alias("ventas_total"))
    d = spark.read.table(table("silver", "devoluciones")).groupBy("producto_id").agg(F.count("devolucion_id").alias("num_devoluciones"), F.sum("monto_reembolso").alias("reembolsos"))
    p = spark.read.table(table("silver", "productos")).select("producto_id", "nombre_producto", "categoria", "marca")
    return p.join(v, "producto_id", "left").join(d, "producto_id", "left").fillna({"unidades": 0, "num_devoluciones": 0})


@dp.materialized_view(name=table("gold", "dotacion_actual"))
def gold_dotacion_actual():
    return (spark.read.table(table("silver", "empleados_hist"))
            .filter("esta_activo AND sucursal_id IS NOT NULL")
            .groupBy("sucursal_id").agg(F.countDistinct("id_empleado").alias("empleados_activos")))


@dp.materialized_view(name=table("gold", "resenas_categoria"))
def gold_resenas_categoria():
    r = spark.read.table(table("silver", "resenas"))
    p = spark.read.table(table("silver", "productos")).select("producto_id", "categoria")
    return (r.join(p, "producto_id", "left").groupBy("categoria")
            .agg(F.count("resena_id").alias("num_resenas"),
                 F.sum(F.when(F.col("calificacion") <= 2, 1).otherwise(0)).alias("negativas"),
                 F.avg(F.when(F.col("calificacion") <= 2, 1.0).otherwise(0.0)).alias("tasa_negativa")))


@dp.materialized_view(name=table("gold", "tracking_estado"))
def gold_tracking_estado():
    return spark.read.table(table("silver", "tracking")).groupBy("estado_entrega", "courier").agg(F.count("tracking_id").alias("envios"))
