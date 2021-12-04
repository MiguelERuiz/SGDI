from mrjob.job import MRJob

class MRHappiness(MRJob):

    def mapper(self, _key, line):
        [word, _happiness_rank,
            happiness_average, _happiness_standard_deviation,
            twitter_rank, _google_rank, _nyt_rank, _lyrics_rank] = line.split()
        happiness_average_f = float(happiness_average)
        if twitter_rank != '--' and happiness_average_f < 2:
            yield(None, (happiness_average_f, word))

    def combiner(self, key, values):
        sorted_values = sorted(values, reverse=True)[:5]
        for item in sorted_values:
            yield(None, item)


    def reducer(self, key, values):
        sorted_values = sorted(values, reverse=True)[:5]
        for item in sorted_values:
            yield(item[1], item[0])

if __name__ == '__main__':
    MRHappiness.run()