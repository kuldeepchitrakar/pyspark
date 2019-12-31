from collections import OrderedDict
from tabulate import tabulate

class JobContext(object):
    def __init__(self, spark):
        self.counters = OrderedDict()
        self._init_accumulators(spark)
        self._init_shared_data(spark)

    def _init_accumulators(self, spark):
        pass

    def _init_shared_data(self, spark):
        pass

    def initalize_counter(self, spark, name):
        self.counters[name] = spark.sparkContext.accumulator(0)

    def inc_counter(self, name, value=1):
        if name not in self.counters:
            raise ValueError("%s counter was not initialized. (%s)" % (name, self.counters.keys()))

        self.counters[name] += value

    def print_accumulators(self):
        print(tabulate(self.counters.items(), self.counters.keys(), tablefmt="simple"))