# SGDI, Práctica MapReduce - Apache Spark, Sergio García Sánchez, Miguel 
# Declaramos que esta solución es fruto exclusivamente de nuestro tra-
# bajo personal. No hemos sido ayudados por ninguna otra persona ni
# hemos obtenido la solución de fuentes externas, y tampoco hemos
# compartido nuestra solución con otras personas. Declaramos además
# que no hemos realizado de manera deshonesta ninguna otra actividad
# que pueda mejorar nuestros resultados ni perjudicar los resultados de
# los demás.
from mrjob.job import MRJob
import json
import re
from collections import Counter

class Space(MRJob):

    def mapper(self, key, line):
        lineJson = json.loads(line)
        content = self.clean(lineJson["content"])
        palabras = content.split()
        conteo = Counter(palabras)
        for p in palabras:
            yield(p, (conteo[p], lineJson["filename"]))

    def clean(self, line):
        return re.sub("([^\w\s]|\_*)", "", line).lower()


    def combiner(self, key, values):
        ls = sorted(list(values), reverse=True)[:1]
        for item in ls:
            yield (key, item)

    def reducer(self, key, values):
        ls = sorted(list(values), reverse=True)[:1]
        for item in ls:
            yield (key, (item[1], item[0]))


if __name__ == '__main__':
    Space.run()