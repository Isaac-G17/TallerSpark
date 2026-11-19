from pyspark.sql import functions as F

def perfilar(df, clave, tipos):
    def contar(condicion):
        return F.count(F.when(condicion, 1))

    clave_limpia = F.trim(F.col(clave))
    metricas = [
        F.count("*").alias("filas"),
        F.countDistinct(
            F.when(clave_limpia != "", clave_limpia)
        ).alias("claves_distintas"),
    ]

    for nombre in df.columns:
        columna = F.col(nombre)
        limpia = F.trim(columna)
        metricas.extend([
            contar(columna.isNull()).alias(f"{nombre}_nulos"),
            contar(limpia == "").alias(f"{nombre}_vacios"),
        ])

        if nombre in tipos:
            convertido = F.expr(f"try_cast(trim(`{nombre}`) AS {tipos[nombre]})")
            metricas.append(
                contar(
                    columna.isNotNull() & (limpia != "") & convertido.isNull()
                ).alias(f"{nombre}_conversiones_fallidas")
            )

    return df.agg(*metricas)