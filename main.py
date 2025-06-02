from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
import boto3
import pandas as pd
import subprocess
import os
from mapreduce import ProductoMasCaroPorCategoria

app = FastAPI()

# Rutas en S3
CLUSTER_ID = "j-1D511EL70WVJD"  # ID de tu clúster EMR
S3_SCRIPT = "s3://ha-doop/scripts/job-runner.sh"
INPUT_S3 = "s3://ha-doop/input/productos.csv"
OUTPUT_S3 = "s3://ha-doop/output/"
TMP_S3 = "s3://ha-doop/tmp/"
LOCAL_OUTPUT_FILE = "s3://ha-doop/output/resultado.txt"

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

        df = pd.read_csv(LOCAL_OUTPUT_FILE, header=None, names=["categoria", "producto_mas_caro"])
        df = df.where(pd.notnull(df), None)  # Fix NaN issue
        return JSONResponse(content=df.to_dict(orient="records"))

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/descargar")
def descargar_csv():
    if os.path.exists(LOCAL_OUTPUT_FILE):
        return FileResponse(LOCAL_OUTPUT_FILE, media_type="text/csv", filename="resultados.csv")
    else:
        return JSONResponse(status_code=404, content={"error": "Archivo CSV no disponible"})
