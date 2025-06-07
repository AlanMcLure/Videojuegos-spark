# Imagen base con Java y Python
FROM openjdk:17-slim

# Instala Python y dependencias básicas
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Instala PySpark (compatible con Hadoop embebido)
RUN pip install pyspark

# Crea directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos del proyecto al contenedor
COPY . /app

# Comando por defecto para ejecutar el script de prueba
CMD ["python3", "test_spark.py"]
