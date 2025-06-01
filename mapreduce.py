#!/usr/bin/env python3.11

from mrjob.job import MRJob
import csv
import sys

class ProductoMasCaroPorCategoria(MRJob):

    def mapper(self, _, line):
        try:
            for row in csv.reader([line]):
                # Evitar procesar la fila del encabezado
                if row[0].strip().lower() == "id":
                    return
                category = row[4].strip()
                price = float(row[2].strip())
                title = row[1].strip()
                yield category, (price, title)
        except Exception as e:
            sys.stderr.write(f"Error en línea: {line}\n")
            sys.stderr.write(f"{e}\n")

    def reducer(self, category, values):
        max_price, max_title = 0.0, ""
        for price, title in values:
            if price > max_price:
                max_price = price
                max_title = title
        yield category, f"{max_title} (${max_price})"
