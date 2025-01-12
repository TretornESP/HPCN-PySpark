# Como ejecutar los ejercicios

## Preparación

Podemos borrar los resultados de ejecuciones previas con

```bash
rm -rf dfCitas.parquet dfInfo.parquet p2out p3out p4out p5out
```

Asegurate de tener el comando `spark-submit` en tu PATH. Si no lo tienes,
puedes añadirlo con

```bash
export PATH=$PATH:/ruta/a/spark/bin
```

Además deberías tener descargados los ficheros

- `cite75_99.txt`
- `apat63_99.txt`
- `country_codes.txt`

Los puedes obtener desde [este enlace](https://nubeusc-my.sharepoint.com/:u:/g/personal/tf_pena_usc_es/EVmESx6Ux4BCjA4nl916iNkBbpUjuzjnwTqrP_2nvv31rQ?e=mtA6dx).

Es vital ejecutar los comandos en orden, ya que los ejercicios dependen unos de otros.

### Ejercicio 1

```bash
spark-submit --master local[*] --driver-memory 4g p1.py cite75_99.txt apat63_99.txt dfCitas.parquet dfInfo.parquet
```

### Ejercicio 2

```bash
spark-submit --master local[*] --driver-memory 4g p2.py dfCitas.parquet/ dfInfo.parquet/ country_codes.txt p2out
```

### Ejercicio 3

```bash
spark-submit --master local[*] --driver-memory 4g p3.py apat63_99.txt p3out
```

### Ejercicio 4

```bash
spark-submit --master local[*] --driver-memory 4g p4.py dfCitas.parquet/ dfInfo.parquet/ ES,FR p4out
```

### Ejercicio 5

```bash
spark-submit --master local[*] --driver-memory 4g p5.py dfInfo.parquet/ p5out
```

## Script to do everything

```bash
./run.sh [local|yarn]
```