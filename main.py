from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
import boto3
import pandas as pd
import subprocess
import os
from mapreduce import ProductoMasCaroPorCategoria
import numpy as np

app = FastAPI()

# Rutas en S3
CLUSTER_ID = "j-1D511EL70WVJD" 
S3_SCRIPT = "s3://ha-doop/scripts/job-runner.sh"
INPUT_S3 = "s3://ha-doop/input/productos.csv"
OUTPUT_S3 = "s3://ha-doop/output/"
TMP_S3 = "s3://ha-doop/tmp/"
LOCAL_OUTPUT_FILE = "resultado.txt"

@app.get("/")
def read_root():
    return {"mensaje": "API del procesamiento MapReduce"}

@app.post("/procesar")
def ejecutar_mapreduce():
    emr = boto3.client("emr", region_name="us-east-1")

    step = {
        'Name': 'Lanzar Bash MapReduce Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['bash', S3_SCRIPT]
        }
    }

    response = emr.add_job_flow_steps(JobFlowId=CLUSTER_ID, Steps=[step])
    step_id = response['StepIds'][0]

    return JSONResponse({
        "mensaje": "Paso lanzado correctamente",
        "step_id": step_id
    })

@app.get("/resultados")
def obtener_resultados():
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "ha-doop"
    prefix = "output/resultados"

    os.makedirs("resultados", exist_ok=True)

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            s3.download_file(bucket, key, LOCAL_OUTPUT_FILE)
            break

        df = pd.read_csv(LOCAL_OUTPUT_FILE, header=None, names=["categoria", "producto_mas_caro"], encoding='latin1')

        # Reemplazar NaN con None para JSON
        df = df.where(pd.notnull(df), None)
        return JSONResponse(content=df.to_dict(orient="records"))

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.get("/descargar")
def descargar_csv():
    input_file = LOCAL_OUTPUT_FILE
    output_csv = "resultado_convertido.csv"

    if not os.path.exists(input_file):
        raise HTTPException(
            status_code=404,
            detail="Archivo de resultados no disponible. Ejecute primero el procesamiento."
        )

    try:
        # Leer el archivo con manejo robusto de encoding y delimitadores
        df = pd.read_csv(
            input_file,
            header=None,
            names=["categoría", "producto_más_caro"],
            encoding='utf-8',
            sep='\t',  # Hadoop suele usar tabulaciones
            on_bad_lines='skip'
        )

        # Limpieza de datos
        df = df.dropna()
        df = df[df['categoría'] != '']

        # Guardar como CSV con encoding UTF-8 y cabeceras
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')

        # Configurar headers para forzar descarga como CSV
        return FileResponse(
            path=output_csv,
            filename="resultados_productos.csv",
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=resultados_productos.csv",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar el archivo: {str(e)}"
        )

