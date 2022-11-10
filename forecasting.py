from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Pool, cpu_count

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, FILENAME, get_logger

logger = get_logger()


def forecast_weather():
    cities = CITIES.keys()
    workers = len(cities)
    logger.debug("Thread workers count %s", workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = executor.map(DataFetchingTask.fetch, cities)
        data = list(futures)

    workers_cpu = cpu_count() - 1
    logger.debug("Pool workers count %s", workers_cpu)
    manager = Manager()
    queue = manager.Queue()
    calculation = DataCalculationTask(queue=queue)
    aggregation = DataAggregationTask(queue=queue, filename=FILENAME)

    with Pool(processes=workers_cpu) as pool:
        tasks_timeout = len(cities)
        calculation_tasks = pool.map_async(calculation.run, data)
        aggregation_task = pool.apply_async(aggregation.run)
        calculation_tasks.wait(timeout=tasks_timeout)
        if not calculation_tasks.ready():
            logger.error(
                "Tasks calculation doesnt completed after "
                "timeout=%s seconds", tasks_timeout)
            raise TimeoutError
        queue.put(None)
        aggregation_task.wait()

    analyze = DataAnalyzingTask(filename=FILENAME)
    analyze.run()
    most_comfortables = analyze.comfortables
    logger.info("Best place to visit is %s", most_comfortables)


if __name__ == "__main__":
    forecast_weather()
