from mrjob.job import MRJob

class MRWordCount(MRJob):


    def mapper_init(self):
        self.queue = []
        self.max_size = 5

    def mapper(self, key, line):
        line = line.split()

        happiness_average = float(line[2])
        word = line[0]

        if self.check_preconditions(line):
            if len(self.queue) < self.max_size:
                self.queue.append((happiness_average, word))
            elif len(self.queue) >= self.max_size:
                self.remove_min_and_insert(happiness_average, word)

    def check_preconditions(self, line):
        return float(line[2]) < 2 and line[4] != "--"
    
    def remove_min_and_insert(self, happiness_average, word):
        min_elem = min(self.queue)
        if(happiness_average >= min_elem[0]):
            self.queue.remove(min_elem)
            self.queue.append((happiness_average, word))

    def mapper_final(self):
        for item in self.queue:
            yield(None, item)


    def reducer(self, key, values):
        queue = []
        max_size = 5
        
        for item in values:
            queue.append(item)
        queue = sorted(queue, reverse=True)[:max_size]
        for item in queue:
            yield (item[1], item[0])


if __name__ == '__main__':
    MRWordCount.run()