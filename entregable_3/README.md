# Entregable 3

## Setup

Para levantar Airflow necesitamos:

- Configurar el usuario y las carpetas necesarias:

```bash
mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

- Inicializar la base de datos de Airlfow:

```bash
docker compose up airflow-init
```

- Ya estamos en condiciones levantarlo:

```bash
docker compose up
```

Desde `localhost:8080` podemos manejar la consola web.

Para configurar las variables que necesita el DAG:

- Vamos a `Admin` > `Variables`.
- Podemos importar `variables.json` desde el botón `Import Variables` o
  crearlas manualmente, una por una.

## Arquitectura

```mermaid
graph LR
    A[AirflowTask] <-- Read and Write --> B[(LocalStackS3)]
    A -- Submits Jobs --> C[Spark]
    C <-- Read and Write --> B
    C -- Write Final Data -->D[(Redshift)]
    style A fill:##00ccff
    style C fill:#ff9933

```

Tenemos 3 componentes:

- Airflow: orquesta las tareas en el orden esperado.
- Spark: lee los datos de la API, realiza algunas transformaciones
  y carga los datos finales en Redshift.
- LocalStack: para emular S3. Repositorio en el que las tareas
  de Airflow y Spark van a guardar y leer los datos de la API en sus
  distintas etapas. Ya que pasar otra cosa que no sea metadata por
  XCom es una mala idea me pareció oportuno simular una forma
  de uso que podría acercarse más a un DAG en producción.

## DAG

```mermaid
graph LR
    A[create_s3_bucket] --> B[get_motorcycles_data]
    B --> C[transform_motorcycles_data_with_spark]
    C --> D[load_motorcycles_data_in_redshift]
    E[create_redshift_table] --> D
```

Podemos correrlo manualmente desde la UI de Airflow.

## Conclusiones

- Usé la TaskFlow API de Airflow para definir el DAG. Me pareció una gran manera de abstraerse
  de ciertos detalles como los XCom y de usar una forma más pythonesca de definir el DAG, que
  evita romper el LSP(por ejemplo: task1 >> task2 >> task3 al LSP pyright no le gusta nada).
- Me llevo una imagen positiva de Airflow.
- Levantar Spark y hacer que se comunique con Airflow y S3 fue lo más complicado del entregable.
  Como en el entregable 2 creo que se debe más a un error de mi parte: no saber buscar de forma adecuada y no
  estar al día con el mundo de Java. Por ejemplo: Maven y cómo traerse un jar.
  El ejemplo en el repo del curso me ayudó bastante con el setup.
