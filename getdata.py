import requests
import pandas as pd

# Obtener datos desde la API
response = requests.get('https://fakestoreapi.com/products')

# Convertir la respuesta JSON en una lista de diccionarios
productos = response.json()


df = pd.DataFrame(productos)


df.to_csv('productos.csv', index=False, encoding='utf-8')

print("Archivo 'productos.csv' guardado correctamente.")
