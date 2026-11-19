from pyspark.sql import functions as F


def referencias_huerfanas(hijas, padres, clave):
    clave_limpia = F.trim(F.col(clave))
    hijas_con_clave = hijas.where(
        clave_limpia.isNotNull() & (clave_limpia != "")
    ).withColumn(clave, clave_limpia)

    claves_padre = (
        padres.select(clave_limpia.alias(clave))
        .where(F.col(clave).isNotNull() & (F.col(clave) != ""))
        .distinct()
    )

    return hijas_con_clave.join(claves_padre, clave, "left_anti")
