import requests
import pandas as pd

# Obtener datos desde la API
response = requests.get('https://fakestoreapi.com/products')

# Convertir la respuesta JSON en una lista de diccionarios
productos = response.json()

# Convertir a DataFrame de pandas
df = pd.DataFrame(productos)

# Guardar en CSV
df.to_csv('productos.csv', index=False, encoding='utf-8')

print("Archivo 'productos.csv' guardado correctamente.")
