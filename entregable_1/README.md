# Entregable 1

_Nota:_ estoy demasiado acostumbrado al código en 'english'. Sepa disculpar, no
puedo evitarlo.

## API

[API Ninjas Motorcycles](https://api-ninjas.com/api/motorcycles)

Permite consultar datos sobre motos, de distintos fabricantes, por modelo
y año de fabricación. Por ejemplo:

Request: GET https://api.api-ninjas.com/v1/motorcycles?make=Motomel

Response:

```json
[
  {
    "make": "Motomel",
    "model": "Sirius 190",
    "year": "2022",
    "type": "Naked bike",
    "displacement": "198.8 ccm (12.13 cubic inches)",
    "engine": "Single cylinder, four-stroke",
    "power": "15.0 HP (10.9  kW)) @ 8000 RPM",
    "top_speed": "100.0 km/h (62.1 mph)",
    "compression": "9.2:1",
    "bore_stroke": "65.5 x 59.0 mm (2.6 x 2.3 inches)",
    "fuel_system": "Injection",
    "cooling": "Air",
    "transmission": "Chain   (final drive)",
    "front_suspension": "Inverted Telescopic",
    "rear_suspension": "Mono shocks",
    "front_tire": "110/70-17 ",
    "rear_tire": "120/70-17 ",
    "front_brakes": "Single disc",
    "rear_brakes": "Single disc",
    "dry_weight": "128.0 kg (282.2 pounds)",
    "total_height": "1080 mm (42.5 inches)",
    "total_length": "2080 mm (81.9 inches)",
    "total_width": "740 mm (29.1 inches)",
    "fuel_capacity": "16.00 litres (4.23 US gallons)",
    "starter": "Electric & kick"
  }
]
```

Requiere una API-KEY como header y, en la versión gratuita, está limitada a
50000 requests por mes.

## Extracción

Tendremos una lista de fabricantes y de años. Nos quedaremos con los
primeros 30 datos de cada uno de los fabricantes por año.

El script se puede correr de la siguiente manera:

```bash
cd data-eng-coderhouse/entregable_1
python3 extract.py
```

Al finalizar, guardará la información en un archivo JSON.

## Carga

Usaremos el archivo JSON del paso anterior para la carga de datos
en Redshift.

```bash
cd data-eng-coderhouse/entregable_1
python3 load.py
```

## Requerimientos

Es necesario contar con las siguientes librerías de Python:

```bash
pip install requests redshift-connector
```

La versión de Python con la que se probaron los scripts es la 3.10.

Todas las variables sensibles, como las credenciales para Redshift,
se leen como env vars por lo que es necesario definirlas antes de
correr los scripts.

## Posibles mejoras

- El schema puede variar según el fabricante. Podríamos obtener todas
  las características disponibles en los datos extraídos y definir un
  schema con todas las columnas posibles para la tabla de Redshift en
  lugar de priozar un subconjunto de ellas.

- Opté por usar la mínima cantidad posible de librerías externas. Esto
  podría ser un limitante en el futuro, especialmente en la confección
  de las queries a Redshift. En el futuro, usar librerías más robustas
  para hacer esto como SQLAlchemy podrí ser una mejor opción.

- Si bien se escapa del alcance de la tarea: subir los datos a S3 y usar
  el comando COPY para cargar los datos en Redshift. La documentación
  menciona que la forma recomendada de carga de datos masivos consiste
  en utilizar COPY con datos almacenados en S3 y no hacer lo que hicimos
  en el script de carga que es insertar muchas filas por medio de un INSERT.
