import logging
import timeit
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Pool, cpu_count

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, FILENAME

logger = logging.getLogger("forecasting")
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
stream_formatter = logging.Formatter(
    '%(asctime)s - %(funcName)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(stream_formatter)
logger.setLevel(logging.DEBUG)


def forecast_weather():
    cities = CITIES.keys()
    workers = len(cities)
    logger.debug(f"Всего городов и потоковых воркеров {workers}")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = executor.map(DataFetchingTask.fetch, cities)
        data = list(futures)

    workers_cpu = cpu_count() - 1
    logger.debug(f"Количество воркеров для пула == {workers_cpu}")
    manager = Manager()
    queue = manager.Queue()
    calculation = DataCalculationTask(queue)
    aggregation = DataAggregationTask(queue=queue, filename=FILENAME)

    with Pool(processes=workers_cpu) as pool:
        pool.map(calculation.run, data)
        while not queue.empty():
            pool.apply(aggregation.run)
        pool.close()
        pool.join()

    analyze = DataAnalyzingTask(filename=FILENAME)
    analyze.run()
    print(analyze.get_comfortables())


if __name__ == "__main__":
    forecast_weather()
