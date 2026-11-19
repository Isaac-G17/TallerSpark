import argparse
import csv
import json
import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession, functions as F
from perfilamiento import perfilar
from integridad import referencias_huerfanas
from limpieza import limpiar_entradas
from ventas import calcular_ventas, guardar_resumen


def main():
    argumentos = argparse.ArgumentParser(description="ETL local de ventas")
    argumentos.add_argument("--verificar-reproceso", action="store_true")
    verificar = argumentos.parse_args().verificar_reproceso
    proyecto = Path(__file__).resolve().parents[1]
    datos = proyecto / "data"
    try:
        configurar_entorno(proyecto)
    except FileNotFoundError as error:
        argumentos.exit(1, f"{error}\n")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("taller-data-engineer")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        # Los CSV tienen encabezados y Spark lee sus columnas como texto.
        clientes = spark.read.csv((datos / "customers.csv").as_posix(), header=True)
        productos = spark.read.csv((datos / "products.csv").as_posix(), header=True)
        pedidos = spark.read.csv((datos / "orders.csv").as_posix(), header=True)
        detalle = spark.read.csv((datos / "order_items.csv").as_posix(), header=True)

        salida = proyecto / "resultados"
        print("Perfilamiento\n".upper())
        print("Clientes perfilados\n".upper())
        perfil_clientes = perfilar(clientes, "customer_id", {"signup_date": "DATE"})
        perfil_clientes.show(vertical=True)

        print("Productos perfilados\n".upper())
        perfil_productos = perfilar(productos, "product_id", {
            "unit_price": "DECIMAL(12,2)",
            "active": "BOOLEAN",
        })
        perfil_productos.show(vertical=True)

        print("Pedidos perfilados\n".upper())
        perfil_pedidos = perfilar(pedidos, "order_id", {
            "order_ts": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        })
        perfil_pedidos.show(vertical=True)

        print("Detalles perfilados\n".upper())
        perfil_detalle = perfilar(detalle, "line_id", {
            "quantity": "INT",
            "discount_pct": "DECIMAL(10,4)",
        })
        perfil_detalle.show(vertical=True)

        perfiles = {
            "customers": perfil_clientes,
            "products": perfil_productos,
            "orders": perfil_pedidos,
            "order_items": perfil_detalle,
        }
        for tabla, perfil in perfiles.items():
            if not verificar:
                guardar_dataframe(salida / "perfiles" / f"{tabla}.csv", perfil)

        huerfanas = {
            "pedidos_sin_cliente": referencias_huerfanas(
                pedidos, clientes, "customer_id"
            ),
            "lineas_sin_pedido": referencias_huerfanas(
                detalle, pedidos, "order_id"
            ),
            "lineas_sin_producto": referencias_huerfanas(
                detalle, productos, "product_id"
            ),
        }

        conteos_huerfanas = []
        for relacion, filas in huerfanas.items():
            total = filas.count()
            print(relacion, total)
            conteos_huerfanas.append((relacion, total))
        if not verificar:
            guardar_csv(
                salida / "referencias_huerfanas.csv",
                ["relacion", "filas_huerfanas"],
                conteos_huerfanas,
            )

        incremental = datos / "incremental"
        pedidos_incrementales = spark.read.schema(pedidos.schema).json(
            (incremental / "orders_incremental.jsonl").as_posix()
        )
        detalle_incremental = spark.read.schema(detalle.schema).json(
            (incremental / "order_items_incremental.jsonl").as_posix()
        )
        pedidos_consolidados = incorporar_lote(pedidos, pedidos_incrementales, verificar).cache()
        lineas_consolidadas = incorporar_lote(detalle, detalle_incremental, verificar).cache()
        huerfanas_consolidadas = {
            "pedidos_sin_cliente": referencias_huerfanas(pedidos_consolidados, clientes, "customer_id"),
            "lineas_sin_pedido": referencias_huerfanas(lineas_consolidadas, pedidos_consolidados, "order_id"),
            "lineas_sin_producto": referencias_huerfanas(lineas_consolidadas, productos, "product_id"),
        }
        tablas, cuarentena, auditoria = limpiar_entradas(
            clientes, productos, pedidos_consolidados, lineas_consolidadas, huerfanas_consolidadas
        )
        tablas = {nombre: filas.cache() for nombre, filas in tablas.items()}
        output = proyecto / "output"
        calidad = output / "calidad"
        if not verificar:
            guardar_dataframe(calidad / "auditoria.csv", auditoria)
            for tabla, filas in cuarentena.items():
                total = guardar_dataframe(calidad / "cuarentena" / f"{tabla}.csv", filas)
                print(f"{tabla}: {total} filas en cuarentena")

        ventas, elegibles, excluidas = calcular_ventas(tablas)
        control = guardar_resumen(
            spark, ventas.cache(), elegibles, (output / "ventas_mensuales").as_posix(), verificar
        )
        pedidos_vigentes = tablas["orders"]
        conteo = pedidos_vigentes.agg(F.count("*"), F.countDistinct(F.trim("order_id"))).first()
        if conteo[0] != conteo[1]:
            raise ValueError("El estado consolidado tiene pedidos duplicados")
        actualizaciones = (
            pedidos_incrementales.select("order_id").distinct()
            .join(pedidos.select("order_id").distinct(), "order_id")
        )
        versiones = pedidos_vigentes.join(actualizaciones, "order_id").select("order_id", "updated_at")
        fechas_esperadas = (
            pedidos_consolidados.join(actualizaciones, "order_id")
            .groupBy("order_id")
            .agg(F.max(F.expr("try_cast(trim(updated_at) AS TIMESTAMP)")).alias("latest"))
        )
        if versiones.join(fechas_esperadas, "order_id").where(
            ~F.to_timestamp("updated_at").eqNullSafe(F.col("latest"))
        ).limit(1).count():
            raise ValueError("Hay pedidos actualizados que no conservaron la versión más reciente")
        control["pedidos_distintos_antes_de_cuarentena"] = pedidos_consolidados.select("order_id").distinct().count()
        control["pedidos_vigentes"] = conteo[0]
        control["lineas_limpias"] = tablas["order_items"].count()
        control["versiones_de_pedidos_actualizados"] = [
            fila.asDict() for fila in versiones.orderBy("order_id").collect()
        ]
        control["lineas_excluidas_por_estado"] = [
            fila.asDict() for fila in excluidas.orderBy("status").collect()
        ]
        if verificar:
            anterior = json.loads((calidad / "controles.json").read_text(encoding="utf-8"))
            if control != anterior:
                raise ValueError("El reproceso cambió los controles del ETL")
        else:
            (calidad / "controles.json").write_text(
                json.dumps(control, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
        print(json.dumps(control, ensure_ascii=False, indent=2))
        if verificar:
            print("Reproceso correcto: mismos grupos, importes y controles.")
    finally:
        spark.stop()


def guardar_dataframe(ruta, dataframe):
    return guardar_csv(ruta, dataframe.columns, dataframe.toLocalIterator())


def incorporar_lote(historico, lote, verificar):
    consolidado = historico.unionByName(lote)
    return consolidado.unionByName(lote) if verificar else consolidado


def guardar_csv(ruta, columnas, filas):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with ruta.open("w", encoding="utf-8", newline="") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(columnas)
        for fila in filas:
            escritor.writerow(fila)
            total += 1
    return total


def configurar_entorno(proyecto):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if sys.platform == "win32":
        # El lanzador de Spark 4.3.0 usa TEMP sin comillas en Windows.
        os.chdir(proyecto)
        temporal = Path(".venv") / "tmp"
        temporal.mkdir(parents=True, exist_ok=True)
        os.environ["TEMP"] = str(temporal)
        os.environ["TMP"] = str(temporal)
        os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).resolve().parent))
        hadoop_local = proyecto / ".venv" / "hadoop" / "hadoop-3.5.0"
        hadoop_home = os.environ.get("HADOOP_HOME")
        candidatos = [Path(hadoop_home), hadoop_local] if hadoop_home else [hadoop_local]
        for hadoop in candidatos:
            binarios = hadoop.resolve() / "bin"
            if all((binarios / archivo).is_file() for archivo in ("winutils.exe", "hadoop.dll")):
                os.environ["HADOOP_HOME"] = str(binarios.parent)
                os.environ["PATH"] = str(binarios) + os.pathsep + os.environ.get("PATH", "")
                break
        else:
            raise FileNotFoundError(
                "PySpark necesita winutils.exe y hadoop.dll de Hadoop 3.5.0 en Windows. "
                f"Instálalos en {hadoop_local / 'bin'} o configura HADOOP_HOME con la "
                "ruta a la carpeta de Hadoop que contiene bin. Consulta README.md."
            )


if __name__ == "__main__":
    main()
