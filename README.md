# Módulo de ingesta y dimulación ERP - Proyecto BI Desempleo Juvenil Canarias

## 📑 Propósito del repositorio

Este repositorio contiene el motor de ingesta de datos desarrollado para el **Proyecto de Business Intelligence sobre el Desempleo Juvenil en Canarias**. El script actúa como el primer eslabón del pipeline de datos, simulando un sistema transaccional de una organización real a partir de fuentes de datos abiertas del **ISTAC**.

El objetivo primordial es poner en valor la metodología utilizada y el rigor en el tratamiento de la información desde su origen. Este componente ha sido diseñado para:

* **Simulación del entorno operacional**: Actúa como el sistema de origen (`bd_erp`), recreando cómo una organización recolectaría sus registros brutos antes de ser procesados en el Data Warehouse.
* **Automatización de la ingesta**: Descarga y normaliza automáticamente los datasets definidos en la configuración centralizada, integrando fuentes de Paro Registrado, EPA y Tasas de Inserción.
* **Garantía de trazabilidad**: Implementa un registro de auditoría (`erp_ingest_log`) para monitorizar la carga y asegurar la calidad del dato desde la fase inicial.

## 🛠️ Stack tecnológico

El desarrollo se sustenta en una arquitectura de datos moderna y escalable:

* **Lenguaje**: Python 3.x para la generación de scripts y descarga de datos mediante APIs.
* **Base de datos**: PostgreSQL, que desempeña el rol de base de datos operacional simulada (`bd_erp`).
* **Lógica de configuración**: YAML para la definición estructurada de los datasets y sus metadatos.
* **Seguridad**: Gestión de credenciales mediante variables de entorno (`.env`).

## ⚙️ Funcionalidades del script

### 1. Normalización y estandarización
El script incluye funciones de "sanitización" (`snake_case`) que transforman los encabezados originales en nombres de columna normalizados. Esto facilita la posterior integración con herramientas de transformación como **dbt**.

### 2. Gestión automatizada de infraestructura
Utiliza lógica de "infraestructura como código" para:
* Verificar la existencia de la base de datos y crearla con codificación UTF-8 si es necesario.
* Recrear las tablas de forma dinámica (capa `Raw`) basándose en la estructura detectada en los archivos CSV.

### 3. Carga de datos eficiente
Implementa el método `copy_expert` de PostgreSQL para realizar una carga masiva de datos (bulk load) desde STDIN, asegurando un rendimiento óptimo en comparación con inserciones fila por fila.

## 🚀 Guía de uso

1. **Configuración**: Definir las credenciales en el archivo `.env`.
2. **Definición**: Listar los datasets deseados en `datasets.yaml`.
3. **Ejecución**:
   ```bash
   python main.py