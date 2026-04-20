# 📊 Pipeline de Datos – Ingesta Automatizada (Nivel Pro)

## 🧠 Descripción

Este proyecto implementa un pipeline de datos enfocado en la **ingesta automatizada** desde una API pública.
El proceso obtiene datos de personajes de Rick and Morty, los transforma y los almacena en una estructura organizada, aplicando además **ingesta incremental** para evitar duplicados.

---

## 🎯 Objetivo

Simular la primera etapa de un pipeline de datos real, permitiendo:

* Obtener datos desde una fuente externa (API)
* Procesarlos automáticamente
* Almacenarlos de forma estructurada
* Mantenerlos actualizados sin intervención manual

---

## 🌐 Fuente de datos

Se utiliza la API pública:

```
https://rickandmortyapi.com/api/character
```

Los datos se obtienen en formato **JSON** y contienen información de personajes como:

* id
* nombre
* estado
* especie
* origen
* ubicación

---

## ⚙️ Funcionamiento del pipeline

El flujo del proceso es:

```
API → JSON → DataFrame → CSV → data/raw/
```

### Pasos:

1. Se realiza una petición HTTP a la API
2. Se recorren todas las páginas (paginación)
3. Se transforman los datos a formato tabular
4. Se comparan con los datos existentes
5. Se agregan solo nuevos registros (ingesta incremental)
6. Se almacenan en `data/raw/personajes.csv`
7. Se registran logs del proceso

---

## 🔄 Ingesta incremental

El sistema evita duplicados mediante:

* Uso del campo `id` como identificador único
* Comparación entre datos nuevos y existentes

Esto permite:

* Agregar solo nuevos personajes
* Mantener histórico de datos
* Optimizar el proceso

---

## 🤖 Automatización

El script puede ejecutarse automáticamente mediante herramientas del sistema operativo como:

* Task Scheduler (Windows)
* cron (Linux/Mac)

Esto permite que:

* El proceso se ejecute periódicamente
* Los datos se actualicen sin intervención manual

---

## 📁 Estructura del proyecto

```
proyecto_pipeline/
│
├── data/
│   └── raw/
│       └── personajes.csv
│
├── src/
│   └── ingesta.py
│
├── logs/
│   └── ingesta.log
│
├── README.md
└── requirements.txt
```

---

## ▶️ Ejecución

Desde la carpeta raíz del proyecto:

```
python src/ingesta.py
```

---

## 📦 Requisitos

Instalar dependencias:

```
pip install -r requirements.txt
```

Contenido de `requirements.txt`:

```
pandas
requests
```

---

## 📝 Logs

El sistema registra información en:

```
logs/ingesta.log
```

Incluye:

* Inicio del proceso
* Cantidad de registros
* Errores (ej: API 429)

---

## ⚠️ Manejo de errores

El script considera:

* Errores de conexión
* Límite de solicitudes (HTTP 429)
* Archivos inexistentes

Se aplican pausas (`sleep`) para evitar bloqueos de la API.

---

## 🧠 Tecnologías utilizadas

* Python
* pandas
* requests

---

## 📌 Conclusión

Este proyecto implementa un pipeline de ingesta de datos automatizado, integrando:

* Consumo de API
* Procesamiento de datos
* Almacenamiento estructurado
* Ingesta incremental
* Automatización

Representa una solución simplificada pero alineada con prácticas reales de ingeniería de datos.
