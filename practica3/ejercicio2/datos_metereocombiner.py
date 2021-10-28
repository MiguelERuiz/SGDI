from mrjob.job import MRJob
from datetime import datetime
class MRWordCount(MRJob):

    def mapper(self, key, line):
        line = line.split()
        result = {}
        if line[0] != "date-time,atmospheric":
            data = line[1].split(",")
            date = datetime.strptime(line[0], '%Y/%m/%d').strftime("%m/%Y")
            batery = float(data[8])
            result["min"] = batery
            result["max"] = batery
            result["sum"] = batery
            result["total"] = 1

            yield(date, result)

    def combiner(self, key, values):
        min = None
        max = None
        sum = 0
        total = 0
        result = {}
        for value in values:
            if min == None or value["min"] < min:
                min = value["min"]
            if max == None or value["max"] > max:
                max = value["max"]
            total += value["total"]
            sum += value["sum"]
        result["min"] = min
        result["max"] = max
        result["sum"] = sum
        result["total"] = total
        yield(key, result)

    def reducer(self, key, values):
        min = None
        max = None
        sum = 0
        total = 0
        result = {}
        for value in values:
            if min == None or value["min"] < min:
                min = value["min"]
            if max == None or value["max"] > max:
                max = value["max"]
            total += value["total"]
            sum += value["sum"]

        result["max"] = max
        result["avg"] = sum/total
        result["min"] = min
        yield(key, result)


if __name__ == '__main__':
    MRWordCount.run()