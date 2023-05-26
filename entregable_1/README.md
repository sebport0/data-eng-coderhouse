# Entregable 1

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

## Criterio para la extracción de datos

Tendremos una lista de fabricantes y de años. Nos quedaremos con los
primeros 30 datos de cada uno de los fabricantes, por año.

El script(probado con python 3.10) se puede correr de la siguiente manera:

```bash
cd data-eng-coderhouse/entregable_1
python3 extract.py
```

Al finalizar, guardará la información en un archivo JSON. Para poder correrlo
se requiere la librería `requests`:

```bash
pip install requests
```

_Nota:_ estoy demasiado acostumbrado al código en 'english'. Sepa disculpar, no
puedo evitarlo.
