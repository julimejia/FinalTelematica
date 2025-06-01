from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
import subprocess
import pandas as pd
import csv
import os
import io

from job import ProductoMasCaroPorCategoria

app = FastAPI()

HDFS_INPUT_PATH = "/user/datasets/productos.csv"
HDFS_OUTPUT_PATH = "/user/datasets/resultados"
LOCAL_OUTPUT_FILE = "resultados/resultados.csv"

@app.get("/")
def read_root():
    return {"mensaje": "API del procesamiento MapReduce"}

@app.post("/procesar")
def ejecutar_mapreduce():
    # Ejecutar el job con mrjob en modo hadoop (sobre HDFS)
    mr_job = ProductoMasCaroPorCategoria(args=[
        "-r", "hadoop",
        HDFS_INPUT_PATH,
        "--output-dir", HDFS_OUTPUT_PATH
    ])

    with mr_job.make_runner() as runner:
        runner.run()

    # Descargar resultados de HDFS a archivo local
    os.makedirs("resultados", exist_ok=True)
    with open(LOCAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
        subprocess.run(["hdfs", "dfs", "-cat", f"{HDFS_OUTPUT_PATH}/part-*"], stdout=f)

    return {"mensaje": "Procesamiento completado"}

@app.get("/resultados")
def obtener_resultados():
    try:
        df = pd.read_csv(LOCAL_OUTPUT_FILE, header=None, names=["categoria", "producto_mas_caro"])
        return JSONResponse(content=df.to_dict(orient="records"))
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "Resultados no encontrados"})

@app.get("/descargar")
def descargar_csv():
    if os.path.exists(LOCAL_OUTPUT_FILE):
        return FileResponse(LOCAL_OUTPUT_FILE, media_type="text/csv", filename="resultados.csv")
    else:
        return JSONResponse(status_code=404, content={"error": "Archivo CSV no disponible"})
