# ElectroCasa | Proyecto integrador Databricks Associate, edición 14

Proyecto reproducible para integrar ventas, productos, eventos de RR. HH., reseñas, devoluciones y tracking de Azure SQL en Unity Catalog. El repositorio reúne las definiciones de ingesta, transformación, orquestación y gobierno; incluye los datos sintéticos del caso y un perfil reproducible de sus fuentes.

## Estructura

```text
databricks.yml
resources/electrocasa_pipeline.yml
resources/electrocasa_job.yml
src/electrocasa/transformations/pipeline.py
notebooks/00_setup.ipynb
notebooks/01_ingest_snapshots.ipynb
notebooks/02_verify.ipynb
notebooks/03_governance.ipynb
notebooks/04_create_connection.ipynb
data/                         # Muestras sintéticas entregadas en el curso
docs/                         # Perfil de fuentes, consultas y guía operativa
tools/profile_sources.py       # Exploración reproducible de archivos originales
tools/validate_structure.py    # Validación local de estructura y sintaxis
```

## Diseño

| Fuente | Llegada y método | Bronze | Silver / regla crítica | Gold |
| --- | --- | --- | --- | --- |
| Ventas, CSV | Diaria; Auto Loader | Streaming table `ventas` | `venta_id`, sucursal, producto, fecha, cantidad > 0, monto > 0; deduplicación por `venta_id` | `ventas_sucursal_mes`, `productos_desempeno` |
| Productos, JSON | Snapshot poco frecuente; lectura batch y reemplazo managed | `productos` | `precio_lista > 0`, normalización de categoría, última fila por ID | `productos_desempeno`, `resenas_categoria` |
| Empleados, CSV | Eventos por lote; `COPY INTO` incremental e idempotente por archivo | `empleados` | DNI e ID y fecha válidos, eventos deduplicados; historia tipo 2 | `dotacion_actual` |
| Reseñas, JSON | Incremental; Auto Loader con JSON multilínea | Streaming table `resenas` | Calificación 1–5, deduplicación por ID | `resenas_categoria` |
| Devoluciones, CSV | Diaria; Auto Loader | Streaming table `devoluciones` | Reembolso >= 0, deduplicación por ID | `productos_desempeno` |
| Tracking, Azure SQL | Bajo volumen; Lakehouse Federation, lectura de seis columnas y reemplazo snapshot | `tracking` | Estado normalizado, deduplicación por ID | `tracking_estado` |

El job encadena notebook de snapshots y `COPY INTO` → refresh de Lakeflow Spark Declarative Pipelines → verificación → masking y permisos. Las tres fuentes por Auto Loader se cargan *dentro* del pipeline; las otras tres se preparan antes y son fuentes Bronze de las vistas Silver del mismo pipeline. Se usan tablas managed en Bronze, Silver y Gold porque el pipeline administra su ciclo de vida; los archivos de landing se preservan en un Volume managed independiente. Auto Loader escribe su estado de esquema bajo el Volume `bronze.pipeline_state`, fuera de los archivos de entrada; Lakeflow administra los checkpoints del streaming table. El `COPY INTO` registra archivos ya cargados en su tabla Delta: volver a ejecutar el mismo archivo con `force=false` no duplica sus filas. Para una corrección de RR. HH., entregar un archivo nuevo, no sobrescribir el mismo nombre ya consumido.

**Cuarentena:** `silver.cuarentena` registra fuente, motivo, archivo o sistema de origen, lote, hora de ingesta y registro completo. Los `expect_or_drop` impiden que los registros inválidos alcancen Silver; la vista de cuarentena se calcula a partir de Bronze con las mismas condiciones. Requiere acceso exclusivo de Ingeniería porque el payload puede contener DNI, salario o email. La deduplicación de IDs repetidos es una regla de consolidación aparte, no un rechazo por calidad; los duplicados persisten en Bronze.

**Empleados:** historia tipo 2 por `id_empleado` y `fecha_evento`; `vigente_desde` inclusivo, `vigente_hasta` exclusivo, `es_actual` y `esta_activo`. Una baja permanece como última versión inactiva; una transferencia o cambio salarial preserva la versión anterior. Dos eventos diferentes el mismo día requieren orden real del sistema origen para establecer su secuencia inequívoca: el ejemplo usa un orden determinista por tipo y archivo, por lo que la vigencia intradía debe revisarse si aparece ese caso. Un DNI compartido por IDs distintos es una anomalía de origen, no una clave para fusionar empleados.

**Calidad:** política de **descarte con trazabilidad** para monto de venta inválido, precio no positivo, identidad/fecha de empleado incompleta, calificación fuera de 1–5, reembolso negativo y estado de tracking desconocido. Mantenerlos inflaría los indicadores o impediría historizar; se conservan en Bronze y se muestran en cuarentena. Verificar las métricas de las expectativas en el event log del pipeline.

## Exploración reproducible de las muestras

| Fuente | Filas | IDs únicos | Hallazgo puntual |
| --- | ---: | ---: | --- |
| Ventas | 15 225 | 15 000 | 319 montos vacíos, 557 montos no positivos entre los no vacíos, 251 sucursales vacías |
| Productos | 3 030 | 3 000 | Reemisión de 30 ID |
| Empleados | 4 078 | 2 000 | 63 DNI vacíos, 46 fechas de evento vacías; varios eventos por empleado |
| Reseñas | 8 080 | 8 000 | 42 calificaciones nulas |
| Devoluciones | 4 040 | 4 000 | 71 reembolsos negativos |

Estas cifras son del archivo bruto, no de las tablas resultantes. Para tracking, la fuente de ejecución es Azure SQL; el script de muestra incluido en `data/` no sustituye una consulta real al servidor.

## Preparación y despliegue

1. Contar con workspace Azure Databricks, Unity Catalog, permisos de creación de catálogo y un **administrador de cuenta** para los grupos. Ejecutar `notebooks/00_setup.ipynb` para cada catálogo (`electrocasa_dev` y `electrocasa`). Informar `account_id` y autenticar `AccountClient` como admin de cuenta; se crean `*_ingenieria`, `*_analistas`, `*_auditoria`. Los grupos locales de workspace no son válidos para privilegios de Unity Catalog. Asignar miembros reales a esos grupos y acceso al workspace antes de la ejecución.
2. En **cada** catálogo subir los archivos con estos nombres y rutas: `ventas_sucursales.csv` a `.../landing/ventas/`, `catalogo_productos.json` a `.../landing/catalogo/`, `empleados_rrhh.csv` a `.../landing/empleados/`, `resenas_clientes.json` a `.../landing/resenas/` y `devoluciones.csv` a `.../landing/devoluciones/`. El prefijo completo es `/Volumes/<catalogo>/bronze/landing/`. Crear las subcarpetas al cargar los archivos. No subir `tracking_envios_azure_sql.sql` al Volume.
3. Crear un Secret Scope `electrocasa-sql` con claves `username` y `password`. Después, ejecutar `notebooks/04_create_connection.ipynb` para crear la conexión SQL Server con referencias a esos secretos y el catálogo foráneo `electrocasa_sql`. Conceder al ejecutor del job `USE CATALOG`, `USE SCHEMA` y `SELECT` sobre la tabla externa si no es propietario. Verificar conectividad hacia Azure SQL TCP 1433; no hace falta driver JDBC en el notebook serverless. El script SQL sintético se conserva en `data/` como referencia del esquema de `dbo.TrackingEnvios`.
4. Configurar CLI y autenticación del workspace (`databricks auth login --host <workspace-host>`). Sustituir `<correo-real>` por tu dirección real; validar, desplegar y ejecutar el bundle con:

   ```bash
   databricks bundle validate -t dev --var 'alert_email=<correo-real>'
   databricks bundle deploy -t dev --var 'alert_email=<correo-real>'
   databricks bundle run -t dev electrocasa_job --var 'alert_email=<correo-real>'
   databricks bundle validate -t prod --var 'alert_email=<correo-real>'
   databricks bundle deploy -t prod --var 'alert_email=<correo-real>'
   databricks bundle run -t prod electrocasa_job --var 'alert_email=<correo-real>'
   ```

5. Revisar permisos de ejecución del propietario del job sobre ambos catálogos, Volume, secretos y Azure SQL. El schedule diario 07:00 `America/Lima` está **PAUSED** para permitir corridas manuales controladas; activarlo según la frecuencia de actualización requerida. El job reintenta la ingesta y el refresh dos veces, con 60 s de intervalo.

## Gobierno y seguridad

El notebook de setup crea tres **grupos de cuenta**, ejecuta GRANT sobre el catálogo y los schemas y concede acceso al Volume solo a Ingeniería. El último task del job da SELECT a Analistas y Auditoría sobre las cinco tablas Gold y a Ingeniería sobre Silver, incluido el registro de cuarentena. Dos funciones de máscara sobre `silver.empleados_hist.dni` y `.salario` devuelven el dato real solo a miembros del grupo de Ingeniería. Verificar mediante una cuenta de cada grupo; una vista agregada Gold no expone DNI, salario ni email. Para auditoría, el acceso a lineage del catálogo puede depender de los permisos y prestaciones habilitados en el workspace y debe comprobarse allí.

## Verificación y consultas

El perfil de las fuentes se obtiene con `python tools/profile_sources.py`. La estructura y sintaxis del código se comprueban con `python tools/validate_structure.py`; el resultado se encuentra en `docs/validacion_local.txt`. `docs/consultas_gold.sql` reúne consultas de ventas, productos, dotación, reseñas y cuarentena para revisar los resultados del pipeline. Para el monitoreo operativo, consultar el event log de Lakeflow y el historial del job en Databricks.

## Cómputo y costos

El pipeline y los notebooks de Jobs utilizan cómputo serverless, con ejecuciones discretas diarias y sin cluster permanente. El catálogo pequeño se reemplaza en batch; Auto Loader incremental minimiza lecturas repetidas de archivos; COPY INTO evita reprocesar archivos de RR. HH.; tracking se consulta vía Lakehouse Federation solo una vez por job y limita columnas. Para este conjunto de muestras, la ejecución diaria puede ser más frecuente que lo necesario: ajustar a la llegada real y supervisar duración y costo desde Jobs y el event log. No hacer full refresh sin necesidad: reinterpreta los datos de entrada y puede incrementar el gasto.

