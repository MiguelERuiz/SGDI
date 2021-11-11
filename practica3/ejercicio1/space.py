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
        # return re.sub("(\||\.|@|,|\"|\'|>|<|\-|\)|\(|\:|!|\?|\=\+)|\/", "", line).lower()
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