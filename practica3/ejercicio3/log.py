from mrjob.job import MRJob

class MRWordCount(MRJob):

    def mapper(self, key, line):
        self.data = {}
        line = line.split()
        if(len(line) > 1):
            host = line[0]
            error = self.is_error(line[-2])
            bytes = self.calculate_bytes(line[-1])
            
            self.data["total"] = 1
            self.data["num_errors"] = error
            self.data["total_bytes"] = bytes

            yield(host, self.data)

    def is_error(self, http_code):
        if http_code[0] == "4" or http_code[0] == "5":
            return 1
        else:
            return 0

    def calculate_bytes(self, bytes):
        if bytes == "-":
                return 0
        else:
            return int(bytes)

    def combiner(self, key, values):
        self.data = {
            "total" : 0,
            "num_errors" : 0,
            "total_bytes" : 0
            }

        for val in values:
            self.data["total"] += val["total"]
            self.data["num_errors"] += val["num_errors"]
            self.data["total_bytes"] += val["total_bytes"]
        yield (key, self.data)


    def reducer(self, key, values):
        self.data = {
            "total" : 0,
            "num_errors" : 0,
            "total_bytes" : 0
            }
        for val in values:
            self.data["total"] += val["total"]
            self.data["num_errors"] += val["num_errors"]
            self.data["total_bytes"] += val["total_bytes"]
        yield (key, self.data)



if __name__ == '__main__':
    MRWordCount.run()