from mrjob.job import MRJob

class MRWordCount(MRJob):


    def mapper(self, key, line):
        line = line.split()

        happiness_average = float(line[2])
        word = line[0]

        if self.check_preconditions(line):
            yield(None, (happiness_average,word))

    def check_preconditions(self, line):
        return float(line[2]) < 2 and line[4] != "--"

    def combiner(self, key, values):
        max_size = 5

        ls = sorted(list(values), reverse=True)[:max_size]
        for item in ls:
            yield (None, item)



    def reducer(self, key, values):
        max_size = 5

        ls = sorted(list(values), reverse=True)[:max_size]
        for item in ls:
            yield (item[1], item[0])


if __name__ == '__main__':
    MRWordCount.run()