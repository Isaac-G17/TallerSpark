# Taller local de PySpark

La consigna y el Excel original están en `docs/`. El entorno del taller usa Python 3.12, Java 21 mediante `JAVA_HOME` y la versión de PySpark fijada en `requirements.txt`.

```text
src/                 Código del taller
data/                CSV y lotes incrementales en JSONL
docs/                Consigna en PDF y Excel original
resultados/          Perfilamiento y referencias del histórico
output/              Parquet y evidencia de la ejecución consolidada
```

## Preparar y ejecutar en PowerShell

Abre PowerShell en la raíz del proyecto, donde está `requirements.txt`.

1. Comprueba que Python 3.12 y el JDK 21 estén disponibles. Si `JAVA_HOME` está vacío o apunta a otra versión, configúralo con la ruta de tu JDK 21.

   ```powershell
   py -3.12 --version
   & "$env:JAVA_HOME\bin\java.exe" -version
   ```

2. Crea el entorno virtual. Este paso se hace una vez.

   ```powershell
   py -3.12 -m venv .venv
   ```

3. Activa el entorno cada vez que abras una terminal nueva. Al activarlo, PowerShell suele mostrar `(.venv)` en el prompt.

   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

4. Instala las dependencias en el entorno activo.

   ```powershell
   python -m pip install -r requirements.txt
   ```

5. Ejecuta el archivo principal.

   ```powershell
   python .\src\__init__.py
   ```

Si PowerShell impide ejecutar `Activate.ps1`, puedes usar directamente el Python del entorno virtual.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe .\src\__init__.py
```

Los cuatro CSV de `data/` y los dos JSONL de `data/incremental/` se exportaron de las seis hojas del Excel porque faltaba el ZIP de la consigna. Usa `data/` en las rutas que el PDF presenta con `datos/`. La exportación conserva el orden y los valores de las filas, incluidos los duplicados y los campos vacíos.

Para escribir Parquet en Windows con un entorno recién creado, descarga `hadoop-3.5.0.zip`, comprueba su SHA-256 y extráelo en `.venv/hadoop/`. El script detecta los componentes si `winutils.exe` y `hadoop.dll` quedan en `.venv/hadoop/hadoop-3.5.0/bin/`. [Compilación comunitaria de Hadoop para Windows](https://github.com/zehelh/winutils/releases/tag/hadoop-3.5.0).

## Resultados de la entrega

- `output/ventas_mensuales/` contiene el Parquet con `month`, `country`, `channel`, `category` y `net_sales`. El mes se toma de `order_ts` en UTC. Se calcula el importe con decimales, se redondea cada línea a dos decimales y se incluyen pedidos `delivered` y `shipped`.
- `output/calidad/auditoria.csv` registra la clave, el motivo, la acción y el número de filas afectadas. Se eliminaron duplicados exactos, se corrigió `deliverd` y se conservó un cliente con correo vacío porque el correo no interviene en el cálculo. La cuarentena contiene dos productos con precio vacío o no positivo, tres pedidos con datos inválidos y 840 líneas afectadas por referencias, cantidades, precios o descuentos inválidos. Una fila puede tener varios motivos.
- `output/calidad/controles.json` muestra que las **19.749** líneas elegibles se conservaron tras los cruces y que los **19.749** identificadores de línea son distintos. El Parquet tiene **208** grupos. Tanto el detalle válido como el Parquet suman **48.005.814,29**, con diferencia de **0,00**.

## Verificar el incremental

Ejecuta primero el ETL normal para generar el Parquet. Después ejecuta:

```powershell
.\.venv\Scripts\python.exe .\src\__init__.py --verificar-reproceso
```

La verificación incorpora dos copias del mismo lote al histórico y vuelve a ejecutar la limpieza y los cruces. Se conserva el pedido con mayor `updated_at` por `order_id`. Las líneas se deduplican por `line_id`, y los conflictos de contenido se envían a cuarentena. El script compara todas las filas agregadas con el Parquet publicado y verifica los controles guardados. El lote deja **20.100** pedidos distintos antes de cuarentena. El pedido `O00005` queda con `updated_at = 2026-03-05T12:00:00Z`.
