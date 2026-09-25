-- Ejecutar únicamente cuando el job haya creado las vistas Gold.
SELECT sucursal_id, mes, num_ventas, ventas_total, ticket_promedio
FROM electrocasa_dev.gold.ventas_sucursal_mes
ORDER BY mes DESC, ventas_total DESC;

SELECT producto_id, nombre_producto, unidades, num_devoluciones, reembolsos
FROM electrocasa_dev.gold.productos_desempeno
ORDER BY unidades DESC LIMIT 20;

SELECT sucursal_id, empleados_activos
FROM electrocasa_dev.gold.dotacion_actual
ORDER BY empleados_activos DESC;

SELECT categoria, num_resenas, negativas, tasa_negativa
FROM electrocasa_dev.gold.resenas_categoria
ORDER BY tasa_negativa DESC;

-- Cuarentena: requiere permisos de Ingeniería.
SELECT fuente, motivo, count(*) AS registros
FROM electrocasa_dev.silver.cuarentena
GROUP BY fuente, motivo ORDER BY registros DESC;
