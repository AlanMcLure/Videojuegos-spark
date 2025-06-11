# Documentación Técnica del Proyecto ETL y Dashboard

## 🌐 Descripción General

Este proyecto implementa un pipeline ETL completo para la integración de datos desde varias fuentes de videojuegos:

* **Twitch API** (streams y juegos)
* **HowLongToBeat** (duración de juegos)
* **Pokémon Showdown (vía PokéAPI)** (información de Pokémon)

Los datos se extraen, transforman y cargan en una base de datos **PostgreSQL**. Además, se incluyen dashboards interactivos para analizar los datos desde diferentes perspectivas.

---

## ⚙️ Tecnologías Utilizadas

* **Python** con PySpark
* **Docker** (para PostgreSQL y ETL)
* **PostgreSQL** como base de datos destino
* **Streamlit** (opcional para dashboards)
* **Langchain + FastAPI** (opcional si se desea extender a chatbot)

---

## 📚 Estructura del Proyecto

```
.
├── extract/                 # Extracción de datos desde APIs
├── transform/              # Limpieza y transformaciones
├── carga/                   # Carga a base de datos
├── data/                   # Archivos Parquet generados
├── db/
│   └── create_database_structure.sql  # Script para crear estructura en PostgreSQL
├── dashboard/              # Código de visualización Streamlit (opcional)
├── docker-compose.yml      # Orquestación de servicios
├── requirements.txt
└── main.py                 # Orquestador del pipeline ETL
```

---

## ⚡️ Ejecución Rápida (Quickstart)

### 1. Clonar el repositorio

```bash
git clone <REPO_URL>
cd nombre-del-proyecto
```

### 2. Levantar servicios con Docker

```bash
docker-compose up --build
```

Esto lanzará:

* Una base de datos PostgreSQL expuesta en `localhost:5432`
* El contenedor de ETL que ejecutará todo el pipeline al iniciarse

---

## 🎓 Cómo Funciona el Pipeline

### FASE 1: Extracción

Los scripts del módulo `extract/` obtienen los datos de APIs y los convierten en DataFrames de Spark.

### FASE 2: Transformación

En `transform/clean.py` se limpian y homogeneizan los datos:

* Conversión de tipos
* Eliminación de duplicados
* Normalización de campos

### FASE 3: Carga a BD

El módulo `load/to_db.py` se encarga de:

* Eliminar columnas incompatibles con JDBC
* Insertar datos en tablas PostgreSQL con logs de carga

---

## 🔄 Ciclo de Vida del Pipeline

Cada ejecución:

1. Extrae datos frescos de todas las fuentes
2. Los guarda como Parquet (copia limpia)
3. Inserta los datos transformados en la base de datos
4. Registra los logs de carga en la tabla `dw_log_cargas`

---

## 📊 Dashboard y Visualizaciones (opcional con Streamlit)

Puedes lanzar el dashboard si has implementado `dashboard/` con Streamlit:

```bash
streamlit run dashboard/main.py
```

### Ejemplos:

* Twitch: juegos más populares, número de streams por categoría
* HowLongToBeat: juegos más largos, comparación entre modos
* Pokémon: top de estadísticas, distribución por tipo
* Logs: número de cargas por fuente, última carga, errores

---

## ✉️ Contacto / Autores

* Autor: Alan McLure Alarcón
* Curso: Especialización en IA y Big Data
* Fecha: Junio 2025
