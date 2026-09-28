# Pipeline meteorológico de ciudades colombianas

Solución para una prueba técnica de Senior Data Engineering. Extrae datos
históricos de Open-Meteo, valida su calidad, calcula indicadores diarios
y publica archivos Parquet particionados y un resumen mensual para BI.

## Alcance

- Ciudades: Bogotá, Medellín, Barranquilla, Cali y Villavicencio.
- Periodo: 2026-01-01 a 2026-09-15, ambos inclusive.
- Zona horaria de los datos: America/Bogota.
- Granularidad diaria: una fila por ciudad y fecha.
- Granularidad mensual: una fila por ciudad y mes.
- Volumen esperado: 1.290 filas diarias y 45 filas mensuales.
- Septiembre contiene 15 días y se identifica como mes parcial.

Los datos históricos proceden de productos de reanálisis y modelos
meteorológicos de Open-Meteo; no deben interpretarse como mediciones
directas de una estación ubicada exactamente en las coordenadas solicitadas.

## Tecnologías

| Componente | Uso |
|---|---|
| Python 3.12 | Lógica del pipeline |
| Apache Airflow 3.1.7 | Orquestación |
| PostgreSQL 16.9 | Metadatos de Airflow |
| pandas | Transformación y agregaciones |
| PyArrow / Parquet | Almacenamiento columnar |
| DuckDB | Lectura y verificación de particiones |
| requests | Consultas HTTP |
| pytest | Pruebas automatizadas |
| Docker Compose | Entorno local reproducible |

Imagen base: `apache/airflow:3.1.7-python3.12`.
La construcción verificada utiliza Python 3.12.12.
Las dependencias directas están fijadas en `requirements.txt`.

## Requisitos

- Docker Engine y Docker Compose.
- Git.
- Acceso a Internet para descargar imágenes y consultar Open-Meteo.
- Puerto local 8090 disponible.

La configuración de permisos descrita a continuación está orientada a Linux.

## Inicio desde una copia del repositorio

Desde la raíz del proyecto:

```bash
mkdir -p data/raw data/processed data/dashboard data/quality logs runtime
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > .env
docker compose config --quiet
docker compose build
docker compose up -d
```

Consultar el estado:

```bash
docker compose ps
docker compose logs --tail=80 airflow
```

Abrir http://localhost:8090.

Si la interfaz solicita credenciales, consultar el archivo local generado:

```bash
cat runtime/simple_auth_manager_passwords.json.generated
```

Las credenciales generadas no deben añadirse al repositorio.

Comprobar compatibilidad de dependencias:

```bash
docker compose exec airflow python -m pip check
```

## Ejecutar el pipeline

1. Buscar el DAG `weather_open_meteo`.
2. Activarlo si está pausado.
3. Seleccionar `Trigger DAG`.
4. Revisar los estados y logs de sus cinco tareas.

El DAG se ejecuta manualmente (`schedule=None`), tiene `catchup=False`
y permite una sola ejecución activa. Cada tarea tiene hasta dos reintentos,
espera inicial de 60 segundos, espera exponencial limitada a cinco minutos
y tiempo máximo de ejecución de 15 minutos.

Las tareas son:

1. `extract_weather`
2. `validate_raw_data`
3. `transform_weather`
4. `load_parquet`
5. `generate_dashboard_dataset`

Los módulos de negocio no dependen de Airflow. El DAG intercambia rutas
mediante XCom; los datasets se conservan en el volumen compartido.

## Configuración

`config/settings.json` contiene:

- Endpoint de la API.
- Fechas inicial y final.
- Zona horaria.
- Ciudades y coordenadas.
- Variables meteorológicas diarias.
- Unidades y umbrales de indicadores.

La implementación actual requiere Celsius, milímetros y km/h.
Cambiar esas unidades requiere adaptar las reglas y nombres de columnas.

Variables de entorno:

| Variable | Propósito |
|---|---|
| AIRFLOW_UID | Usuario Linux empleado por el contenedor |
| WEATHER_CONFIG_PATH | Ruta de configuración dentro del contenedor |
| WEATHER_DATA_DIR | Directorio de datos dentro del contenedor |
| PYTHONPATH | Ubicación de los módulos Python |

## Servicios y persistencia

- `airflow`: ejecuta los componentes de Airflow mediante `standalone`.
- `postgres`: conserva exclusivamente los metadatos de Airflow.
- `postgres_data`: volumen Docker para PostgreSQL.
- `data/`: respuestas originales, intermedios, Parquet y reportes.
- `logs/`: logs persistentes de tareas.
- `runtime/`: configuración y estado local de Airflow.
- `src/`, `dags/`, `config/` y `tests/`: montajes de solo lectura.

El puerto de Airflow se publica únicamente en `127.0.0.1:8090`.
PostgreSQL no publica un puerto al equipo anfitrión.

`standalone` simplifica esta demostración local. No es la arquitectura
propuesta para un despliegue productivo.

## Organización de salidas

Cada extracción genera un identificador único.

| Ruta relativa a data/ | Contenido |
|---|---|
| raw/extraction_id=ID/ | Manifiesto, respuestas originales y metadatos |
| quality/extraction_id=ID/ | Reportes de validación y transformación |
| staging/extraction_id=ID/daily.parquet | Datos diarios intermedios |
| processed/ID/ | Parquet particionado y marcador _SUCCESS.json |
| dashboard/ID/monthly_weather.parquet | Resumen mensual |
| dashboard/ID/monthly_weather.csv | Resumen mensual para importación |
| dashboard/ID/summary.json | Metadatos del resumen |

Las particiones siguen el formato:

`processed/ID/city=bogota/year=2026/month=01/part-00000.parquet`

Para esta prueba se generan 45 particiones por extracción.
Las claves de partición se recuperan desde los nombres de las carpetas
mediante lectura Hive.

Dentro del contenedor, el directorio de datos es `/opt/project/data`.
En el equipo anfitrión corresponde a `./data`.

Para encontrar los resúmenes generados:

```bash
find data/dashboard -name monthly_weather.parquet
```

Se debe seleccionar una extracción completa para el análisis.
No se deben unir indiscriminadamente todas las carpetas de extracciones:
cada una contiene una nueva copia del periodo completo.

## Reglas de transformación

| Indicador | Regla |
|---|---|
| Amplitud térmica diaria | Temperatura máxima menos mínima |
| Día lluvioso | Precipitación >= 1 mm |
| Día de lluvia intensa | Precipitación >= 20 mm |
| Día de viento fuerte | Viento máximo diario >= 40 km/h |
| Día adverso | Lluvia intensa o viento fuerte |

Los umbrales son convenciones analíticas configurables del ejercicio,
no criterios oficiales de alerta meteorológica.

Se conservan las coordenadas solicitadas y las coordenadas devueltas
por la API. Las fechas meteorológicas son fechas locales; las marcas
temporales de extracción se registran en UTC.

Los valores faltantes no se reemplazan por cero. Los indicadores usan
lógica nullable: verdadero OR desconocido es verdadero; falso OR
desconocido permanece desconocido.

En el resumen mensual:

- La temperatura media es el promedio de las medias diarias disponibles.
- Máximas y mínimas son extremos de los valores diarios disponibles.
- La precipitación acumulada suma los valores conocidos; si todos
  faltan, el resultado permanece nulo.
- Los días de evento cuentan indicadores verdaderos.
- Los porcentajes usan como denominador los indicadores conocidos.
- Se incluyen conteos de días válidos por variable.
- La cobertura calendario compara fechas presentes con días del mes.
- Los totales de meses parciales no se extrapolan.

Para agrupar varios meses, los promedios requieren ponderación por días
válidos. Los porcentajes deben recalcularse como suma de eventos dividida
por suma de días válidos, no promediarse directamente.

## Calidad y manejo de errores

La extracción utiliza tiempos de espera HTTP y hasta tres reintentos
adicionales para solicitudes GET ante errores transitorios, incluidos
429 y determinados errores 5xx. Respeta Retry-After.

Se conserva el cuerpo original de cada respuesta HTTP exitosa,
su URL, fecha de extracción y huella SHA-256. El manifiesto indica
si la extracción está en proceso, completa o fallida.

La validación comprueba:

- Integridad SHA-256.
- Ciudades esperadas.
- Secuencia exacta de fechas y ausencia de duplicados.
- Variables requeridas y longitudes.
- Unidades y zona horaria.
- Valores numéricos finitos.
- Precipitación y viento no negativos.
- Coherencia entre temperaturas mínima, media y máxima.
- Conteos de valores faltantes.

Los errores bloquean las etapas posteriores. Los faltantes se reportan
como advertencias y se conservan para el análisis.

La carga escribe primero en un directorio temporal, verifica con DuckDB
el total de filas y las claves únicas, y publica la carpeta completa
mediante un renombrado en el mismo sistema de archivos.

Repetir la carga de la misma entrada reutiliza el dataset publicado.
Si cambia el archivo intermedio para el mismo identificador, la carga
falla para evitar sobrescribir silenciosamente la publicación.

## Pruebas

```bash
docker compose exec airflow python -m pytest \
  /opt/project/tests -v -p no:cacheprovider
```

Se verificaron 12 casos:

- Límites de los indicadores.
- Propagación de faltantes.
- Denominadores de métricas mensuales.
- Precipitación totalmente desconocida.
- Mes parcial y mes completo.
- Fechas válidas, duplicadas y fuera del periodo.
- Modificación de archivos raw.
- Repetición de carga sin duplicados.
- Rechazo de cambios sobre una extracción publicada.

## Resultados verificados

En la ejecución revisada:

- Cinco tareas del DAG terminaron correctamente.
- 1.290 filas diarias, sin faltantes en las variables solicitadas.
- 45 particiones y 1.290 claves ciudad-fecha únicas.
- 45 filas mensuales.
- Cinco registros ciudad-mes parciales, correspondientes a septiembre.
- 885 registros ciudad-día lluviosos.
- 87 registros ciudad-día con lluvia intensa.
- Cero registros ciudad-día con viento >= 40 km/h.
- 87 registros ciudad-día adversos.
- 12 pruebas aprobadas.

Los conteos corresponden a registros ciudad-día, no a fechas únicas
del calendario. Una nueva consulta podría reflejar revisiones de la fuente.

## Consumo de BI

Importar `dashboard/ID/monthly_weather.csv` o utilizar un conector
compatible con el archivo Parquet correspondiente.

Seleccionar una sola extracción. Usar `month_start` como fecha,
`city_name` como dimensión e `is_partial_month` para identificar
periodos incompletos.

Visualizaciones propuestas:

- Líneas de temperatura media por mes y ciudad.
- Barras de precipitación acumulada por ciudad para el mismo periodo.
- Barras de porcentaje de días adversos.
- Mapa de ciudades con precipitación o temperatura.
- Tabla de cobertura y días válidos.

Se recomienda comparar septiembre mediante tasas o promedios diarios
y señalar explícitamente que incluye únicamente 15 días.

## Limitaciones y mejoras productivas

- Procesamiento local y en memoria, adecuado para el volumen del ejercicio.
- Particiones pequeñas justificadas por el requisito de la prueba;
  a mayor escala conviene revisar su tamaño y estrategia.
- Dependencias directas fijadas, sin bloqueo completo de transitivas.
- Imágenes fijadas por versión, no por digest.
- Cada nueva extracción descarga el periodo completo.
- La carga es idempotente para la misma extracción; no existe un upsert
  global entre distintas extracciones.
- La verificación de una publicación existente comprueba conteos,
  claves y fechas, pero no hashes individuales de cada Parquet.
- La escritura del resumen mensual es atómica por archivo, no como
  conjunto transaccional de CSV, Parquet y metadatos.
- No hay alertas externas ni políticas automáticas de retención.
- Las pruebas HTTP con respuestas simuladas quedan como mejora.
- No se aplican umbrales específicos por ciudad ni alertas oficiales.
- Para producción: separar servicios Airflow, gestionar secretos,
  fijar dependencias transitivas, añadir CI, almacenamiento de objetos,
  observabilidad, pruebas HTTP y publicación transaccional de datasets.

## Detener el entorno

```bash
docker compose down
```

Este comando conserva el volumen de PostgreSQL y los archivos del proyecto.

## Referencias

- https://open-meteo.com/en/docs/historical-weather-api
- https://airflow.apache.org/docs/apache-airflow/3.1.7/
- https://duckdb.org/docs/stable/data/parquet/overview.html

## Uso de IA

Se utilizó inteligencia artificial como apoyo para elaborar código,
pruebas y documentación. La solución se ejecutó y verificó en el entorno
local mediante el DAG de Airflow y las pruebas automatizadas.

## Consulta de demostración

Ejecutar:

```bash
docker compose exec airflow python -m weather_pipeline.demo
```

Selecciona la extracción más reciente con publicación diaria y archivos
mensuales completos. Muestra el resumen por ciudad y los meses parciales.

Para seleccionar una extracción concreta:

```bash
docker compose exec airflow python -m weather_pipeline.demo \
  --extraction-id ID
```

## Entrega de datos

Los archivos de datos se entregan por separado en un ZIP de una sola
extracción, con las carpetas raw, quality, processed y dashboard.

El código y la documentación se conservan en Git. Los directorios data,
logs, runtime y entregables están excluidos del repositorio.
