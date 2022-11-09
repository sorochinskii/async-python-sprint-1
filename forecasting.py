import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Pool, cpu_count

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES, FILENAME

logger = logging.getLogger("forecasting")
# TODO всю настройку логгера было бы неплохо обернуть в функцию
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
    # не пиши логи с кириллицей. Такие логи не каждый сервер отобразит +
    # русский текст примерно на 15% длиннее английского. А в масшатабах логов
    # это серьезный перерасход дискового пространства
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = executor.map(DataFetchingTask.fetch, cities)
        data = list(futures)

    workers_cpu = cpu_count() - 1
    logger.debug(f"Количество воркеров для пула == {workers_cpu}")
    # параметры более эффективно задавать по-другому, чтобы строка собиралась
    # только при совпадении уровня логгирования логгера с уровнем логируемого
    # сообщения. Подробнее:
    # https://docs.python.org/3/howto/logging.html#logging-variable-data
    manager = Manager()
    queue = manager.Queue()
    calculation = DataCalculationTask(queue)
    aggregation = DataAggregationTask(queue=queue, filename=FILENAME)

    with Pool(processes=workers_cpu) as pool:
        pool.map(calculation.run, data)
        while not queue.empty():
            pool.apply(aggregation.run)
            # TODO сейчас ты фактически бесполезно используешь очередь, так как
            # накидываешь в нее сообщения, а разгребаешь только после того,
            # как все таски с вычислением завершатся (с таким же успехом мог
            # складывать все в список). Рекомендую тебе здесь переписать на
            # map_async. Тогда ты сможешь асинхронно проводить вычисление и
            # сразу же из очереди подхватывать их (то есть этот while надо
            # перенести в DataAggregationTask.run). А сигналом для завершения
            # цикла в DataAggregationTask будет, например, None в очереди.
            # Примерно так:
            # with Pool(processes=workers_cpu) as pool:
            #     tasks = pool.map_async(calculation.run, data)
            #     aggregation_task = apply_async(aggregation.run)
            #     # wait for tasks to complete
            #     tasks.wait()
            #     queue.put(None)
            #     aggregation_task.wait()
        pool.close()
        pool.join()

    analyze = DataAnalyzingTask(filename=FILENAME)
    analyze.run()
    print(analyze.get_comfortables())
    # TODO давай везде использовать logger


if __name__ == "__main__":
    forecast_weather()
