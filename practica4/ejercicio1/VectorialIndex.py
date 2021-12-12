# SGDI, Práctica 4: Recuperación de la información, Sergio García Sánchez, Miguel Emilio Ruiz Nieto
# Declaramos que esta solución es fruto exclusivamente de nuestro trabajo personal.
# No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solución de fuentes
# externas, y tampoco hemos compartido nuestra solución con otras personas. Declaramos
# además que no hemos realizado de manera deshonesta ninguna otra actividad que pueda
# mejorar nuestros resultados ni perjudicar los resultados de los demás.

from collections import Counter
import string
import os
from math import log2, sqrt

# Dada una linea de texto, devuelve una lista de palabras no vacias
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0,
    map(lambda x: x.lower().strip(string.punctuation), linea.split()))


class VectorialIndex(object):


    def __init__(self, directorio):
        self.fileNumber = 0
        self.index = dict()
        self.pathToFile = dict()
        self.documentWeight = dict()

        for root, dirs, files in os.walk(directorio):
            for file in files :
                self.pathToFile[self.fileNumber] = root + "/" + file
                self.documentWeight[self.fileNumber] = 0
                with open(root + "/" + file, 'r', encoding="ISO-8859-1", errors='replace') as file:
                    for line in file:
                        for word in extrae_palabras(line):
                            if word not in self.index:
                                self.index[word] = [self.fileNumber]
                            else:
                                self.index[word].append(self.fileNumber)
                self.fileNumber += 1
        for word, files in self.index.items():
            counter = Counter(files)
            ni = len(counter)
            n = self.fileNumber
            pesos = []
            for file in counter:
                tfidf = (1 + log2(counter[file])) * log2(n/ni)
                self.documentWeight[file] += tfidf * tfidf
                pesos.append([file, tfidf])
            self.index[word] = pesos

    def consulta_vectorial(self, consulta, n=3):
        words = extrae_palabras(consulta)
        for word in words:
            if word not in self.index:
                return
        weightsInQuery = self.weightsInQuery(consulta)
        result = []
        for document, _ in weightsInQuery.items() :
            result.append((self.pathToFile[document], weightsInQuery[document] / sqrt(self.documentWeight[document])))

        return sorted(result, key=lambda pair: pair[1], reverse=True)[0:n]

    def weightsInQuery(self, consulta):
        weights = dict()
        for word in extrae_palabras(consulta):
            for document, weight in self.index[word]:
                if document not in weights:
                    weights[document] = weight
                else:
                    weights[document] += weight
        return weights


    def consulta_conjuncion(self, consulta):
        words = list(extrae_palabras(consulta))
        if(len(words)) == 1:
            words.append(words[0])

        indexWords = []
        for word in words:
            if word not in self.index:
                return
            else:
                indexWords.append(self.index[word])

        documents = self.interesc(indexWords)
        return [ self.pathToFile[document] for document in documents ]

    def interesc(self, indexWords):
        terms = sorted(indexWords, key=len)
        answer = terms[0]
        terms = terms[1:]
        while terms != [] and answer != []:
            e = terms[0]
            answer = self.interesc2(answer, e)
            terms = terms[1:]
        return answer

    def interesc2(self, list1, list2):
        answer = []
        while list1 != [] and list2 != []:
            if(self.docId(list1) == self.docId(list2)):
                answer.append(self.docId(list1))
                list1 = list1[1:]
                list2 = list2[1:]
            elif (self.docId(list1) < self.docId(list2)):
                list1 = list1[1:]
            else:
                list2 = list2[1:]
        return answer

    def docId(self, list):
        if isinstance(list[0], float):
            return list[0]
        else:
            return list[0][0]

if __name__ == '__main__':
    vectorialIndex = VectorialIndex("20news-18828")
    # vectorialIndex = VectorialIndex("test")

    # print(vectorialIndex.consulta_vectorial("DES Diffie-Hellman", n=5))

    print(vectorialIndex.consulta_conjuncion("DES Diffie-Hellman"))
    # print(vectorialIndex.consulta_conjuncion("hola"))
