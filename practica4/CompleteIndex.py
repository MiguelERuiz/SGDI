# Insertar aqui la cabecera

import string
import os
from collections import OrderedDict
# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0, 
    map(lambda x: x.lower().strip(string.punctuation), linea.split()))


class CompleteIndex(object):

    def __init__(self, directorio, compresion=None):
        self.fileNumber = 0
        self.index = dict()
        self.pathToFile = dict()
        self.wordsInFile = dict()
        for root, dirs, files in os.walk(directorio):
                    for file in files :
                        self.pathToFile[self.fileNumber] = root + "/" + file
                        self.wordsInFile[self.fileNumber] = 0
                        with open(root + "/" + file, 'r', encoding="ISO-8859-1", errors='replace') as file:
                            for line in file:
                                for word in extrae_palabras(line):
                                    self.wordsInFile[self.fileNumber] += 1

                                    if word not in self.index:
                                        self.index[word] = OrderedDict()
                                    if self.fileNumber not in self.index[word]:
                                        self.index[word][self.fileNumber] = []
                                    self.index[word][self.fileNumber].append(self.wordsInFile[self.fileNumber])

                        self.fileNumber += 1
        for word, files in self.index.items():
            result = []
            for idFichero, listaApariciones in files.items():
                result.append([idFichero, listaApariciones])
            self.index[word] = result
        # print(self.index)

    def consulta_frase(self, frase):
        words = list(extrae_palabras(frase))
        indexWords = []
        for word in words:
            if word not in self.index:
                return
            else:
                indexWords.append(self.index[word])
        documents = self.intersect_phrase(indexWords)
        return [ self.pathToFile[document] for document in documents ]

    def intersect_phrase(self, indexWords):
        answer = []
        while self.allNotEmpty(indexWords):
            if self.sameDocId(indexWords):
                if self.consecutive(indexWords):
                    answer.append(indexWords[0][0][0])
            indexWords = self.advanceMin(indexWords)
        return answer
    
    def allNotEmpty(self, indexWords):
        for l in indexWords:
            if len(l) == 0:
                return False
        return True
    
    def sameDocId(self, indexWords):
        headers = [l[0][0] for l in indexWords]
        for h in headers:
            if h != headers[0]:
                return False
        return True
    
    def consecutive(self, indexWords):
        while True:
            headers = [l[0][1][0] for l in indexWords if len(l[0][1]) > 0]
            if len(headers) == len(indexWords):
                i = 0
                consecutive = True
                while i < (len(headers) - 1) and consecutive:
                    if headers[i] + 1 != headers[i+1]:
                        consecutive = False
                    i = i + 1
                if consecutive:
                    return True
                else:
                    firstOcurrence = min(headers)
                    for l in indexWords:
                        if l[0][1][0] == firstOcurrence:
                            l[0][1] = l[0][1][1:]
            else:
                return False
    
    def advanceMin(self, indexWords):
        headers = [l[0][0] for l in indexWords]
        menorDocId = min(headers)
        result = []
        for l in indexWords:
            if l[0][0] == menorDocId:
                result.append(l[1:])
            else:
                result.append(l)
        return result

if __name__ == '__main__':
    # completeIndex = CompleteIndex("test")
    completeIndex = CompleteIndex("20news-18828")
    print(completeIndex.consulta_frase("either terrestrial or alien"))
    print(completeIndex.consulta_frase("is more complicated"))
    # print(completeIndex.consulta_frase("tardes amigo tal"))
