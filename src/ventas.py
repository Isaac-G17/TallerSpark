from pyspark.sql import functions as F


ESTADOS_VENTA = ("shipped", "delivered")


def calcular_ventas(tablas):
    clientes = tablas["customers"].select(
        F.trim("customer_id").alias("customer_id"), "country"
    )
    productos = tablas["products"].select(
        F.trim("product_id").alias("product_id"),
        "category",
        F.trim("unit_price").cast("decimal(12,2)").alias("unit_price"),
    )
    pedidos = tablas["orders"].select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trunc(F.to_date(F.to_timestamp("order_ts")), "month").alias("month"),
        "channel",
        "status",
    )
    lineas = tablas["order_items"].select(
        F.trim("line_id").alias("line_id"),
        F.trim("order_id").alias("order_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("quantity").cast("int").alias("quantity"),
        F.trim("discount_pct").cast("decimal(18,8)").alias("discount_pct"),
    )
    pedidos_venta = pedidos.where(F.col("status").isin(*ESTADOS_VENTA))
    elegibles = lineas.join(pedidos_venta.select("order_id"), "order_id", "left_semi")
    excluidas = (
        lineas.join(pedidos.select("order_id", "status"), "order_id")
        .where(~F.coalesce(F.col("status").isin(*ESTADOS_VENTA), F.lit(False)))
        .groupBy("status").count()
    )
    detalle = (
        elegibles.join(pedidos_venta.drop("status"), "order_id")
        .join(clientes, "customer_id")
        .join(productos, "product_id")
        .withColumn(
            "net_amount",
            F.round(
                F.col("quantity") * F.col("unit_price")
                * (1 - F.col("discount_pct")).cast("decimal(9,8)"),
                2,
            ).cast("decimal(25,2)"),
        )
    )
    return detalle, elegibles, excluidas


def guardar_resumen(spark, detalle, elegibles, ruta, verificar):
    control = detalle.agg(
        F.count("*").alias("filas"),
        F.countDistinct("line_id").alias("lineas_distintas"),
        F.count("net_amount").alias("importes_validos"),
        F.coalesce(F.sum("net_amount"), F.lit(0)).alias("total"),
    ).first()
    filas_elegibles = elegibles.count()
    if not filas_elegibles == control.filas == control.lineas_distintas == control.importes_validos:
        raise ValueError(
            f"Cardinalidad inválida: {filas_elegibles} líneas elegibles, "
            f"{control.filas} filas, {control.lineas_distintas} claves y "
            f"{control.importes_validos} importes tras los cruces"
        )

    dimensiones = ["month", "country", "channel", "category"]
    resumen = detalle.groupBy(*dimensiones).agg(F.sum("net_amount").alias("net_sales"))
    if not verificar:
        resumen.write.mode("overwrite").parquet(ruta)
    publicado = spark.read.parquet(ruta)
    if verificar:
        diferencias = resumen.exceptAll(publicado).unionByName(publicado.exceptAll(resumen))
        if diferencias.limit(1).count():
            raise ValueError("El reproceso cambió el Parquet por grupo")
    agregados = publicado.agg(
        F.count("*").alias("grupos"),
        F.coalesce(F.sum("net_sales"), F.lit(0)).alias("total"),
    ).first()
    if control.total != agregados.total:
        raise ValueError(f"Conciliación fallida: detalle {control.total}, Parquet {agregados.total}")
    publicado.orderBy(*dimensiones).show(10, truncate=False)
    return {
        "lineas_elegibles": filas_elegibles,
        "filas_detalle": control.filas,
        "lineas_distintas": control.lineas_distintas,
        "grupos_parquet": agregados.grupos,
        "total_detalle": str(control.total),
        "total_parquet": str(agregados.total),
        "diferencia": str(control.total - agregados.total),
    }
