from mrjob.job import MRJob
import csv

class ProductoMasCaroPorCategoria(MRJob):

    def mapper(self, _, line):
        for row in csv.reader([line]):
            if row[0] == "id":
                return
            try:
                category = row[4]
                price = float(row[2])
                title = row[1]
                yield category, (price, title)
            except:
                pass

    def reducer(self, category, values):
        max_price, max_title = 0, ""
        for price, title in values:
            if price > max_price:
                max_price = price
                max_title = title
        yield category, f"{max_title} (${max_price})"
