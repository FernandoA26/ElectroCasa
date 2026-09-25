# Cómo ejecutar ElectroCasa

## Antes de comenzar

Solicita primero al docente la URL del workspace Azure Databricks del curso, acceso a Unity Catalog, permisos de administración sobre catálogos y grupos, y confirmación de disponibilidad del Azure SQL después del 24-09-2026. Si existe un workspace del curso, no necesitas abrir tu propia suscripción Azure. Databricks Free Edition permite practicar, pero carece de APIs de cuenta necesarias para los grupos del enunciado y limita la red saliente. Para ejecutar íntegramente el proyecto por tu cuenta necesitas un workspace Azure Databricks con Unity Catalog, serverless y permisos de administración. La promoción de 14 días solo garantiza DBUs de Databricks gratuitas, y crear un workspace de Azure exige una suscripción que no sea de prueba gratuita.

## Preparación en Databricks

1. Descomprime el ZIP conservando `databricks.yml` en la raíz de la carpeta `electrocasa-lakehouse`.
2. En la interfaz del workspace, importa `notebooks/00_setup.ipynb` desde Workspace > Import. Configura el widget `catalog=electrocasa_dev`. La primera celda crea catálogo, schemas y Volumes. La segunda crea tres grupos **de cuenta** y necesita `account_id` y autenticación de administrador de cuenta. La tercera asigna permisos y necesita privilegios GRANT. Si no dispones de ellos, solicita al administrador que ejecute esas celdas.
3. En Catalog comprueba que existan `electrocasa_dev.bronze`, `.silver`, `.gold`, `bronze.landing` y `bronze.pipeline_state`.
4. Abre el Volume `electrocasa_dev.bronze.landing`. Crea las subcarpetas siguientes y usa Add data > Upload files to a volume.

| Carpeta | Archivo de `data/` |
| --- | --- |
| `ventas` | `ventas_sucursales.csv` |
| `catalogo` | `catalogo_productos.json` |
| `empleados` | `empleados_rrhh.csv` |
| `resenas` | `resenas_clientes.json` |
| `devoluciones` | `devoluciones.csv` |

El archivo SQL es una muestra y no se carga al Volume.

## Azure SQL y secretos

Instala e inicia sesión en la CLI (paso siguiente). Crea el scope y dos secretos interactivos. Al ejecutarse `put-secret`, la CLI solicitará el valor sin escribirlo en el historial:

```powershell
databricks secrets create-scope electrocasa-sql
databricks secrets put-secret electrocasa-sql username
databricks secrets put-secret electrocasa-sql password
```

Importa `notebooks/04_create_connection.ipynb` y ejecútalo con privilegios CREATE CONNECTION y CREATE CATALOG. Crea una conexión SQL Server y el catálogo foráneo `electrocasa_sql`, que consulta `electrocasadb.dbo.TrackingEnvios`. Su última celda prueba acceso real. Si hay un error de red/firewall o permisos, pide al docente que habilite acceso desde el workspace y conceda a la identidad ejecutora USE CATALOG, USE SCHEMA y SELECT. No se necesita driver JDBC en el notebook serverless.

## CLI en Windows y primera corrida

En PowerShell:

```powershell
winget install Databricks.DatabricksCLI
```

Cierra y vuelve a abrir PowerShell. Entra a la carpeta descomprimida que contiene `databricks.yml` y sustituye los marcadores:

```powershell
cd "C:\ruta\a\electrocasa-lakehouse"
databricks version
databricks auth login --host "https://<url-de-tu-workspace>"
databricks bundle validate -t dev --var "alert_email=<tu-correo-real>"
databricks bundle deploy -t dev --var "alert_email=<tu-correo-real>"
databricks bundle run -t dev electrocasa_job --var "alert_email=<tu-correo-real>"
```

El `bundle run` inicia el job aun si su horario aparece PAUSED. Comprueba en Jobs & Pipelines que completen estas cuatro tareas: `ingest_snapshots`, `refresh_medallion`, `verify_outputs` y `apply_governance`. Revisa tablas Bronze, Silver, Gold y `silver.cuarentena`. Guarda capturas de validate, deploy, job, event log, consultas Gold y grants sin secretos.

Una vez comprobado `dev`, repite `00_setup` con `catalog=electrocasa`, sube los archivos al Volume del catálogo `electrocasa` y ejecuta `validate`, `deploy` y `run` cambiando `-t dev` por `-t prod`. El catálogo foráneo puede usarse desde ambos targets en el mismo metastore. Publica el repositorio en GitHub e incorpora los registros de operación pertinentes.

Ante un error, conserva el texto del mensaje y el nombre de la tarea para aislar la causa antes de continuar.
