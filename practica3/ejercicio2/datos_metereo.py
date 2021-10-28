from mrjob.job import MRJob
from datetime import datetime
class MRWordCount(MRJob):

    def mapper(self, key, line):
        line = line.split()
        if line[0] != "date-time":
            data = line[1].split(",")
            date = datetime.strptime(line[0], '%Y/%m/%d').strftime("%m/%Y")
            yield(date, float(data[8]))

    def reducer(self, key, values):
        min = None
        max = None
        sum = 0
        total = 0
        result = {}
        for value in values:
            if min == None or value < min:
                min = value
            if max == None or value > max:
                max = value
            total += 1
            sum += value

        result["max"] = max
        result["avg"] = sum/total
        result["min"] = min
        yield(key, result)


if __name__ == '__main__':
    MRWordCount.run()