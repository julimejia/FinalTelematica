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
    LOCAL_OUTPUT_FILE = "resultados/downloaded_file.csv"  # Explicit path

    os.makedirs("resultados", exist_ok=True)

    try:
        # 1. Download the file
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if not response.get('Contents'):
            return JSONResponse(
                status_code=404,
                content={"error": "No files found in S3 path"}
            )

        # Get first matching file
        obj = response['Contents'][0]
        s3.download_file(bucket, obj['Key'], LOCAL_OUTPUT_FILE)

        # 2. Read with proper encoding and cleaning
        with open(LOCAL_OUTPUT_FILE, 'rb') as f:
            # Detect encoding automatically
            rawdata = f.read(1024)
            encoding = chardet.detect(rawdata)['encoding'] or 'utf-8'

        # Read CSV with multiple fallback options
        try:
            df = pd.read_csv(
                LOCAL_OUTPUT_FILE,
                encoding=encoding,
                header=None,
                names=["categoria", "producto_mas_caro"],
                on_bad_lines='skip',
                na_values=['', 'null', 'NULL', 'NA', 'N/A', '\\N'],
                keep_default_na=False
            )
        except UnicodeDecodeError:
            # Fallback to latin1 if detected encoding fails
            df = pd.read_csv(
                LOCAL_OUTPUT_FILE,
                encoding='latin1',
                header=None,
                names=["categoria", "producto_mas_caro"],
                on_bad_lines='skip'
            )

        # 3. Clean the data
        df = df.dropna(how='all')  # Remove completely empty rows
        df = df.fillna('')  # Replace remaining nulls with empty string
        
        # Clean special characters from categoria column
        df['categoria'] = df['categoria'].str.replace(r'[^\w\s]', '', regex=True)
        df['categoria'] = df['categoria'].str.strip()

        # 4. Return cleaned data
        return JSONResponse(
            content=df.to_dict(orient="records"),
            media_type="application/json; charset=utf-8"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Processing error: {str(e)}"}
        )

@app.get("/descargar")
def descargar_csv():
    # Archivo de entrada (txt descargado desde S3)
    input_file = LOCAL_OUTPUT_FILE
    output_csv = "resultado_convertido.csv"

    if os.path.exists(input_file):
        try:
            # Leer el txt (puedes ajustar el delimitador según tu salida real)
            df = pd.read_csv(input_file, header=None, names=["categoria", "producto_mas_caro"], encoding='utf-8', delimiter="\t")

            # Guardar como CSV real
            df.to_csv(output_csv, index=False)

            # Devolver como archivo CSV
            return FileResponse(output_csv, media_type="text/csv", filename="resultado.csv")

        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    else:
        return JSONResponse(status_code=404, content={"error": "Archivo de resultados no disponible"})

