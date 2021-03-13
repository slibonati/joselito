import time
import json
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server

class Reader:
   
    def read(self, name):
        inputFile = open(name)
        contents = inputFile.read()
        inputFile.close()
        return contents

class Parser:

    def parse(self, conf, log):
        
        start = log.find('\n', log.find("START_FIO_JSON_OUTPUT"))
        end = log.find("END_FIO_JSON_OUTPUT")
        
        fio = json.loads(log[start:end])

        metrics = list()
        for line in conf.splitlines():
            if not line.startswith("#"):
                values = line.split(",")

                metric = values[0].split('/')
                if len(metric) == 2: # lat_ns/mean
                    metrics.append({
                    "metric": values[0],
                    "help": values[1],
                    "name": values[2],
                    "type": values[3],
                    "value": fio['client_stats'][0]['write'][metric[0]][metric[1]]
                    })
                else:
                    metrics.append({
                    "metric": values[0],
                    "help": values[1],
                    "name": values[2],
                    "type": values[3],
                    "value": fio['client_stats'][0]['write'][metric[0]]
                })

        return metrics

class CustomCollector(object):
    def __init__(self,m):
        self.metrics = m
        # suppress built-in metrics
        for coll in list(REGISTRY._collector_to_names.keys()):
            REGISTRY.unregister(coll)

    def collect(self):
        PREFIX = "fio_write_"
        for item in self.metrics:
            yield GaugeMetricFamily(PREFIX + item['name'], item['help'], item['value'])


if __name__ == '__main__':
    
    r = Reader()
    conf = r.read("fio_metrics.conf")
    fio = r.read("fio.log")
    
    p = Parser()
    metrics = p.parse(conf, fio)
    
    start_http_server(8000)
    REGISTRY.register(CustomCollector(metrics))
    while True:
        time.sleep(1)
