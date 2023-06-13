# Entregable 2

## Setup

Vamos a trabajar con un JupyterLab + PySpark en un container de Docker.
Para levantarlo tenemos que hacer

```
cd data-eng-coderhouse/entregable_2
docker compose build
docker compose up
```

En los logs vamos a tener la URL a JupyterLab con el token de acceso.

Docker toma las variables sensibles desde un archivo `.env`, allí están
definidas las credenciales para conectarse a Redshift y, por conveniencia,
también la env var `PYSPARK_SUBMIT_ARGS` que usamos para pasarle los .jar
a PySpark.

## Datos

La carpeta principal es `entregable_2/data`, que funciona como Docker volume.

En `api/` tenemos el JSON con los datos del entregable anterior y que vamos a cargar con Spark.

En `driver_jdbc/` se encuentran los drivers que le permiten a Spark enchufarse a Redshift.

`notebook.ipynb` es el notebook en el cual vamos a trabajar.
