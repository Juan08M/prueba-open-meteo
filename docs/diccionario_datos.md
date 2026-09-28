# Diccionario de datos

## Convenciones

- Dataset diario: una fila por ciudad y fecha.
- Dataset mensual: una fila por ciudad y mes.
- Las fechas meteorológicas corresponden a America/Bogota.
- Los timestamps de extracción utilizan UTC.
- Los valores desconocidos se conservan como nulos.
- Los tipos descritos son lógicos; el lector puede representar los
  enteros o valores nullable con tipos físicos diferentes.
- Las unidades forman parte del nombre de las métricas.

## Dataset diario

Ubicación: data/processed/ID/city=CIUDAD/year=AAAA/month=MM/*.parquet

Clave única dentro de una extracción: (city, date).

| Campo | Tipo lógico | Descripción |
|---|---|---|
| date | Fecha | Día meteorológico local; almacenado como timestamp sin zona a medianoche |
| city | Texto | Identificador normalizado de ciudad; clave de partición |
| city_name | Texto | Nombre de la ciudad para visualización |
| department | Texto | Departamento según la configuración del ejercicio |
| latitude | Decimal | Latitud solicitada |
| longitude | Decimal | Longitud solicitada |
| source_latitude | Decimal | Latitud devuelta por la API |
| source_longitude | Decimal | Longitud devuelta por la API |
| year | Entero | Año; clave de partición |
| month | Entero | Mes de 1 a 12; clave de partición |
| temperature_mean_c | Decimal nullable | Temperatura media diaria, °C |
| temperature_max_c | Decimal nullable | Temperatura máxima diaria, °C |
| temperature_min_c | Decimal nullable | Temperatura mínima diaria, °C |
| temperature_range_c | Decimal nullable | Máxima menos mínima diaria, °C |
| precipitation_mm | Decimal nullable | Precipitación acumulada diaria, mm |
| wind_speed_max_kmh | Decimal nullable | Velocidad máxima diaria del viento a 10 m, km/h |
| is_rainy_day | Booleano nullable | Precipitación >= umbral de día lluvioso |
| is_heavy_rain_day | Booleano nullable | Precipitación >= umbral de lluvia intensa |
| is_strong_wind_day | Booleano nullable | Viento máximo >= umbral de viento fuerte |
| is_adverse_day | Booleano nullable | Lluvia intensa o viento fuerte |
| has_missing_weather | Booleano | Falta al menos una de las cinco variables meteorológicas base |
| extraction_id | Texto | Identificador de la extracción |
| extracted_at_utc | Timestamp UTC | Momento de extracción de la ciudad |
| source | Texto | Fuente: open-meteo |
| timezone | Texto | Zona horaria de los datos |

Las columnas city, year y month se recuperan desde las carpetas
mediante particionamiento Hive; no se repiten dentro de cada archivo.

## Dataset mensual

Ubicación: data/dashboard/ID/monthly_weather.parquet
Alternativa: data/dashboard/ID/monthly_weather.csv

Clave única dentro de una extracción: (city, month_start).

### Dimensiones, cobertura y trazabilidad

| Campo | Tipo lógico | Descripción |
|---|---|---|
| city | Texto | Identificador normalizado |
| city_name | Texto | Nombre para visualización |
| department | Texto | Departamento configurado |
| latitude | Decimal | Latitud solicitada |
| longitude | Decimal | Longitud solicitada |
| month_start | Fecha | Primer día del mes |
| year | Entero | Año |
| month | Entero | Mes |
| first_observed_date | Fecha | Primera fecha presente en el grupo |
| last_observed_date | Fecha | Última fecha presente en el grupo |
| days_observed | Entero | Número de fechas distintas presentes |
| calendar_days | Entero | Días del mes calendario |
| calendar_coverage_pct | Decimal | 100 × days_observed / calendar_days |
| is_partial_month | Booleano | Hay menos fechas presentes que días calendario |
| days_with_missing_weather | Entero | Filas con alguna variable base faltante |
| extraction_id | Texto | Extracción de origen |

La cobertura calendario no mide la completitud de las variables.
Septiembre de 2026 tiene 15 fechas presentes de 30 días: cobertura
calendario de 50 %, aunque puede tener todas las variables completas
para los 15 días solicitados.

### Métricas meteorológicas

| Campo | Tipo lógico | Agregación diaria |
|---|---|---|
| temperature_mean_c | Decimal nullable | Promedio de temperaturas medias conocidas |
| temperature_max_c | Decimal nullable | Máximo de temperaturas máximas conocidas |
| temperature_min_c | Decimal nullable | Mínimo de temperaturas mínimas conocidas |
| temperature_range_mean_c | Decimal nullable | Promedio de amplitudes térmicas conocidas |
| precipitation_total_mm | Decimal nullable | Suma de precipitación conocida; nulo si no hay valores |
| precipitation_mean_daily_mm | Decimal nullable | Promedio de precipitación de días con valor conocido |
| wind_speed_max_kmh | Decimal nullable | Máximo del viento máximo diario conocido |

### Días válidos por variable

Todos son conteos enteros de valores no nulos.

| Campo | Variable evaluada |
|---|---|
| temperature_mean_c_valid_days | Temperatura media diaria |
| temperature_max_c_valid_days | Temperatura máxima diaria |
| temperature_min_c_valid_days | Temperatura mínima diaria |
| precipitation_mm_valid_days | Precipitación diaria |
| wind_speed_max_kmh_valid_days | Viento máximo diario |

El resumen no incluye un denominador específico para la amplitud
térmica. Si hay faltantes y se necesita agregar esa media entre meses,
debe recalcularse desde el dataset diario.

### Indicadores de eventos

| Conteo | Días conocidos | Porcentaje |
|---|---|---|
| rainy_days | rainy_days_valid_days | rainy_days_pct |
| heavy_rain_days | heavy_rain_days_valid_days | heavy_rain_days_pct |
| strong_wind_days | strong_wind_days_valid_days | strong_wind_days_pct |
| adverse_days | adverse_days_valid_days | adverse_days_pct |

Para cada fila de la tabla anterior:

- Conteo: número de indicadores verdaderos; nulo si todos son desconocidos.
- Días conocidos: número de indicadores no nulos.
- Porcentaje: 100 × conteo / días conocidos; nulo si el denominador es cero.
- Los conteos son enteros lógicos nullable; un lector puede representarlos
  como decimales cuando coexisten con nulos.

Los umbrales iniciales son 1 mm, 20 mm y 40 km/h, respectivamente.
Un día adverso cumple lluvia intensa o viento fuerte.

## Reglas para consumo y agregación

1. Seleccionar una sola extracción para evitar duplicar el periodo.
2. Comparar ciudades usando el mismo intervalo.
3. No comparar un total de septiembre parcial como si fuera mensual completo.
4. Recalcular porcentajes agrupados usando sus numeradores y denominadores.
5. Ponderar la temperatura media por temperature_mean_c_valid_days.
6. Para temperatura máxima, usar MAX; para mínima, MIN.
7. La precipitación acumulada es aditiva en el tiempo para una misma ciudad.
8. Sumar precipitación entre ciudades no representa una lámina de lluvia
   regional; mostrarla por ciudad o definir explícitamente otra estadística.
9. Sumar días de evento entre ciudades produce registros ciudad-día,
   no fechas únicas ni días regionales.
10. La ausencia de valores no equivale a ausencia de lluvia o viento.
11. CSV no conserva tipos: configurar fechas, booleanos y números al importarlo.
