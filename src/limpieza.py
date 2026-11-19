from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F


def _ausente(campo):
    columna = F.col(campo)
    return columna.isNull() | (F.trim(columna) == "")


def _conversion(campo, tipo):
    return F.expr(f"try_cast(trim(`{campo}`) AS {tipo})")


def _registrar(df, tabla, clave, motivo, accion):
    return (
        df.groupBy(clave)
        .count()
        .select(
            F.lit(tabla).alias("tabla"),
            F.col(clave).alias("clave"),
            F.lit(motivo).alias("motivo"),
            F.lit(accion).alias("accion"),
            F.col("count").alias("filas_afectadas"),
        )
    )


def _clasificar(df, tabla, clave, reglas):
    """Separa las filas y registra cada motivo que se cumple."""
    motivos = F.array_compact(F.array(*[
        F.when(condicion, F.lit(motivo))
        for motivo, condicion in reglas.items()
    ]))
    evaluadas = df.withColumn("_motivos", motivos)
    rechazadas = evaluadas.where(F.size("_motivos") > 0)
    auditoria = (
        rechazadas.select(F.col(clave).alias("clave"), F.explode("_motivos").alias("motivo"))
        .groupBy("clave", "motivo")
        .count()
        .select(
            F.lit(tabla).alias("tabla"),
            "clave",
            "motivo",
            F.lit("cuarentena").alias("accion"),
            F.col("count").alias("filas_afectadas"),
        )
    )
    return (
        evaluadas.where(F.size("_motivos") == 0).select(*df.columns),
        rechazadas.select(*df.columns),
        auditoria,
    )


def _marcar_referencia(filas, referencias, clave, marca):
    """Marca coincidencias por clave sin multiplicar las filas de entrada."""
    claves = (
        referencias.select(F.trim(F.col(clave)).alias("_referencia"))
        .where(~_ausente("_referencia"))
        .distinct()
        .withColumn(marca, F.lit(True))
    )
    return (
        filas.join(claves, F.trim(filas[clave]) == claves["_referencia"], "left")
        .drop("_referencia")
    )


def _limpiar_clientes(clientes):
    clave = "customer_id"
    unicos = clientes.distinct()
    duplicados = _registrar(
        clientes.exceptAll(unicos), "customers", clave, "DUPLICADO_EXACTO", "eliminar"
    )
    versiones = Window.partitionBy(F.trim(F.col(clave)))
    evaluados = unicos.withColumn("_versiones", F.count("*").over(versiones))
    limpios, cuarentena, rechazos = _clasificar(evaluados, "customers", clave, {
        "CUSTOMER_ID_AUSENTE": _ausente(clave),
        "CLIENTE_DUPLICADO_CONFLICTIVO": ~_ausente(clave) & (F.col("_versiones") > 1),
    })
    emails = _registrar(
        limpios.where(_ausente("email")), "customers", clave, "EMAIL_AUSENTE", "conservar"
    )
    return limpios.select(*clientes.columns), cuarentena.select(*clientes.columns), [
        duplicados, rechazos, emails
    ]


def _limpiar_productos(productos):
    unicos, duplicados = _preparar_claves(productos, "products", "product_id")
    precio = _conversion("unit_price", "DECIMAL(12,2)")
    limpios, cuarentena, rechazos = _clasificar(unicos, "products", "product_id", {
        "PRODUCT_ID_AUSENTE": _ausente("product_id"),
        "PRODUCTO_DUPLICADO_CONFLICTIVO": F.col("_versiones") > 1,
        "PRECIO_AUSENTE": _ausente("unit_price"),
        "PRECIO_NO_CONVERTIBLE": ~_ausente("unit_price") & precio.isNull(),
        "PRECIO_NO_POSITIVO": precio <= 0,
    })
    return limpios.select(*productos.columns), cuarentena.select(*productos.columns), [
        duplicados, rechazos
    ]


def _preparar_claves(df, tabla, clave):
    unicos = df.distinct()
    duplicados = _registrar(
        df.exceptAll(unicos), tabla, clave, "DUPLICADO_EXACTO", "eliminar"
    )
    versiones = Window.partitionBy(F.trim(F.col(clave)))
    return unicos.withColumn("_versiones", F.count("*").over(versiones)), duplicados


def _limpiar_pedidos(pedidos, clientes_huerfanos, clientes_rechazados):
    clave = "order_id"
    unicos = pedidos.distinct()
    duplicados = _registrar(
        pedidos.exceptAll(unicos), "orders", clave, "DUPLICADO_EXACTO", "eliminar"
    )

    por_pedido = Window.partitionBy(F.trim(F.col(clave)))
    versiones = (
        unicos.withColumn("_actualizado", _conversion("updated_at", "TIMESTAMP"))
        .withColumn("_ultima_fecha", F.max("_actualizado").over(por_pedido))
        .withColumn("_fechas_invalidas", F.count(
            F.when(F.col("_actualizado").isNull(), 1)
        ).over(por_pedido))
        .withColumn("_versiones_recientes", F.count(
            F.when(F.col("_actualizado") == F.col("_ultima_fecha"), 1)
        ).over(por_pedido))
    )
    candidatos, conflictos, rechazos_version = _clasificar(versiones, "orders", clave, {
        "ORDER_ID_AUSENTE": _ausente(clave),
        "UPDATED_AT_INVALIDO_EN_EL_PEDIDO": ~_ausente(clave) & (F.col("_fechas_invalidas") > 0),
        "EMPATE_CONFLICTIVO_UPDATED_AT": ~_ausente(clave) & (F.col("_versiones_recientes") > 1),
    })
    recientes = F.col("_actualizado") == F.col("_ultima_fecha")
    antiguas = _registrar(
        candidatos.where(~recientes), "orders", clave, "VERSION_ANTERIOR", "eliminar"
    )
    vigentes = candidatos.where(recientes).select(*pedidos.columns)
    vigentes = _marcar_referencia(
        vigentes, clientes_huerfanos, "customer_id", "_cliente_inexistente"
    )
    vigentes = _marcar_referencia(
        vigentes, clientes_rechazados, "customer_id", "_cliente_rechazado"
    )
    fecha_pedido = _conversion("order_ts", "TIMESTAMP")
    limpios, invalidos, rechazos_pedido = _clasificar(vigentes, "orders", clave, {
        "CUSTOMER_ID_AUSENTE": _ausente("customer_id"),
        "CLIENTE_INEXISTENTE": F.col("_cliente_inexistente"),
        "CLIENTE_EN_CUARENTENA": F.col("_cliente_rechazado"),
        "ORDER_TS_AUSENTE": _ausente("order_ts"),
        "ORDER_TS_NO_CONVERTIBLE": ~_ausente("order_ts") & fecha_pedido.isNull(),
    })
    estado_errado = F.col("status") == "deliverd"
    correcciones = _registrar(
        limpios.where(estado_errado), "orders", clave, "DELIVERD_A_DELIVERED", "corregir"
    )
    limpios = limpios.withColumn(
        "status", F.when(estado_errado, F.lit("delivered")).otherwise(F.col("status"))
    )
    cuarentena = conflictos.select(*pedidos.columns).unionByName(invalidos.select(*pedidos.columns))
    return limpios.select(*pedidos.columns), cuarentena, [
        duplicados, antiguas, rechazos_version, rechazos_pedido, correcciones
    ]


def _limpiar_lineas(detalle, huerfanas, pedidos_rechazados, productos_rechazados):
    unicos, duplicados = _preparar_claves(detalle, "order_items", "line_id")
    evaluadas = _marcar_referencia(
        unicos, huerfanas["lineas_sin_pedido"], "order_id", "_pedido_inexistente"
    )
    evaluadas = _marcar_referencia(
        evaluadas, huerfanas["lineas_sin_producto"], "product_id", "_producto_inexistente"
    )
    evaluadas = _marcar_referencia(
        evaluadas, pedidos_rechazados, "order_id", "_pedido_rechazado"
    )
    evaluadas = _marcar_referencia(
        evaluadas, productos_rechazados.where(_ausente("unit_price")), "product_id",
        "_precio_ausente"
    )
    evaluadas = _marcar_referencia(
        evaluadas, productos_rechazados.where(~_ausente("unit_price")), "product_id",
        "_precio_invalido"
    )
    cantidad = _conversion("quantity", "INT")
    descuento = _conversion("discount_pct", "DECIMAL(18,8)")
    limpios, cuarentena, auditoria = _clasificar(evaluadas, "order_items", "line_id", {
        "LINE_ID_AUSENTE": _ausente("line_id"),
        "LINEA_DUPLICADA_CONFLICTIVA": F.col("_versiones") > 1,
        "ORDER_ID_AUSENTE": _ausente("order_id"),
        "PRODUCT_ID_AUSENTE": _ausente("product_id"),
        "PEDIDO_INEXISTENTE": F.col("_pedido_inexistente"),
        "PRODUCTO_INEXISTENTE": F.col("_producto_inexistente"),
        "PEDIDO_EN_CUARENTENA": F.col("_pedido_rechazado"),
        "PRECIO_DE_PRODUCTO_AUSENTE": F.col("_precio_ausente"),
        "PRODUCTO_EN_CUARENTENA": F.col("_precio_invalido"),
        "CANTIDAD_AUSENTE": _ausente("quantity"),
        "CANTIDAD_NO_CONVERTIBLE": ~_ausente("quantity") & cantidad.isNull(),
        "CANTIDAD_NO_POSITIVA": cantidad <= 0,
        "DESCUENTO_AUSENTE": _ausente("discount_pct"),
        "DESCUENTO_NO_CONVERTIBLE": ~_ausente("discount_pct") & descuento.isNull(),
        "DESCUENTO_FUERA_DE_RANGO": (descuento < 0) | (descuento > 1),
    })
    return limpios.select(*detalle.columns), cuarentena.select(*detalle.columns), [
        duplicados, auditoria
    ]


def limpiar_entradas(clientes, productos, pedidos, detalle, huerfanas):
    """Devuelve tablas limpias, filas en cuarentena y auditoría por regla."""
    clientes_limpios, clientes_q, auditoria_clientes = _limpiar_clientes(clientes)
    productos_limpios, productos_q, auditoria_productos = _limpiar_productos(productos)
    clientes_q = clientes_q.cache()
    productos_q = productos_q.cache()
    pedidos_limpios, pedidos_q, auditoria_pedidos = _limpiar_pedidos(
        pedidos, huerfanas["pedidos_sin_cliente"], clientes_q
    )
    pedidos_q = pedidos_q.cache()
    lineas_limpias, lineas_q, auditoria_lineas = _limpiar_lineas(
        detalle, huerfanas, pedidos_q, productos_q
    )
    tablas_limpias = {
        "customers": clientes_limpios,
        "products": productos_limpios,
        "orders": pedidos_limpios,
        "order_items": lineas_limpias,
    }
    cuarentena = {
        "customers": clientes_q,
        "products": productos_q,
        "orders": pedidos_q,
        "order_items": lineas_q,
    }
    registros = auditoria_clientes + auditoria_productos + auditoria_pedidos + auditoria_lineas
    auditoria = reduce(lambda actual, siguiente: actual.unionByName(siguiente), registros)
    return tablas_limpias, cuarentena, auditoria
