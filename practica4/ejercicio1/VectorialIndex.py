# Insertar aqui la cabecera

import string
from pathlib import Path

# Dada una linea de texto, devuelve una lista de palabras no vacias
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0,
    map(lambda x: x.lower().strip(string.punctuation), linea.split()))

# Recorre el directorio recursivamente y calcula el indice inverso
def recorre_directorio(directorio):
    entradas_dir = Path(directorio)
    for entrada in entradas_dir.iterdir():
        if entrada.is_dir():
            recorre_directorio(entrada)
        else:
            with open(entrada, 'r') as f:
                # TODO hay que sacar el indice
                print(f.read())


class VectorialIndex(object):

    def __init__(self, directorio):
        recorre_directorio(directorio)

    def consulta_vectorial(self, consulta, n=3):
        pass

    def consulta_conjuncion(self, consulta):
        pass



