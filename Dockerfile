# Imagen base con Spark y Java ya configurado
FROM bitnami/spark:latest

# Cambiar al usuario root para poder instalar dependencias
USER root

# Instalar Python y pip
RUN apt-get update && apt-get install -y python3 python3-pip wget curl

# Crear carpeta de jars y descargar el driver JDBC PostgreSQL
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -O /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Copiar archivo de requerimientos e instalar dependencias Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Crear carpeta de trabajo y copiar el resto del código
WORKDIR /app
COPY . /app

# Añadir utilidad wait-for-it para esperar a que PostgreSQL esté lista
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Comando por defecto al iniciar el contenedor (espera a que la DB esté lista)
CMD bash -c "/wait-for-it.sh db:5432 --timeout=100 --strict -- python3 main.py"
