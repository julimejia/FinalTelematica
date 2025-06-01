#!/bin/bash

# Configuración
BUCKET="ha-doop"
CSV="productos.csv"
SCRIPT="mapreduce.py"
OUTPUT_FILE="resultado.txt"

# 1. Configuración inicial
echo "🔹 1. Configurando entorno..."
hadoop fs -mkdir -p /user/hadoop/input 2>/dev/null

# 2. Descarga de archivos con verificación
echo "🔹 2. Descargando archivos desde S3..."
if ! aws s3 cp "s3://ha-doop/input/${CSV}" .; then
    echo "❌ Error: No se pudo descargar ${CSV} desde S3"
    exit 1
fi

if ! aws s3 cp "s3://${BUCKET}/scripts/${SCRIPT}" .; then
    echo "❌ Error: No se pudo descargar ${SCRIPT} desde S3"
    exit 1
fi

# 3. Subida a HDFS con verificación
echo "🔹 3. Subiendo datos a HDFS..."
if ! hadoop fs -put -f "${CSV}" "/user/hadoop/input/"; then
    echo "❌ Error: No se pudo subir ${CSV} a HDFS"
    exit 1
fi

# 4. Instalación de dependencias
echo "🔹 4. Instalando dependencias..."
sudo yum install -y python3.11 python3.11-pip || {
    echo "❌ Error: No se pudo instalar Python"
    exit 1
}

python3.11 -m pip install --upgrade pip --user || {
    echo "❌ Error: No se pudo actualizar pip"
    exit 1
}

python3.11 -m pip install --user mrjob || {
    echo "❌ Error: No se pudo instalar mrjob"
    exit 1
}

# 5. Configuración del PATH
export PATH=$PATH:/home/hadoop/.local/bin

# 6. Ejecución del trabajo MapReduce
echo "🔹 5. Ejecutando MapReduce..."
if ! python3.11 "${SCRIPT}" -r hadoop "hdfs:///user/hadoop/input/${CSV}" --output-dir "hdfs:///user/hadoop/output/"; then
    echo "❌ Error: Fallo en la ejecución de MapReduce"
    exit 1
fi

# 7. Recuperación de resultados
echo "🔹 6. Recuperando resultados..."
hadoop fs -getmerge "/user/hadoop/output/part-*" "${OUTPUT_FILE}" || {
    echo "❌ Error: No se pudieron obtener los resultados"
    exit 1
}

# 8. Subida de resultados a S3
echo "🔹 7. Subiendo resultados a S3..."
if ! aws s3 cp "${OUTPUT_FILE}" "s3://${BUCKET}/output/"; then
    echo "❌ Error: No se pudo subir el resultado a S3"
    exit 1
fi

echo "✅ Trabajo completado exitosamente! Resultado en s3://${BUCKET}/output/${OUTPUT_FILE}"