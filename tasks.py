import logging
import operator
import os
from multiprocessing import Queue, current_process
from threading import current_thread
from typing import List, Tuple, Union

import pandas as pd
from pandas import DataFrame as data_frame
from pandas.errors import EmptyDataError
from pydantic import parse_obj_as

from api_client import YandexWeatherAPI as YWAPI
from models import CalculationResultModel, ForecastsCityModel, ForecastsModel
from utils import BOTTOM, TOP

logger = logging.getLogger("forecasting")


class DataFetchingTask:
    @staticmethod
    def fetch(city: str) -> ForecastsCityModel:
        logger.debug(f"Запрос к API city=={city}"
                     + f"Из треда name={current_thread().name}")
        result = parse_obj_as(ForecastsModel, YWAPI().get_forecasting(city))
        return ForecastsCityModel(city=city, forecasts=result)


class DataCalculationTask:
    def __init__(self, queue: Union[Queue, None] = None):
        super().__init__()
        self.queue = queue

    def run(self, data):
        result = self._calc(data)
        if self.queue:
            self.queue.put(result)
        else:
            return result

    def _calc(self, data: ForecastsCityModel):
        # Датафрейм для итогов по дням.
        forecasts = data.forecasts.dict()
        city_name = data.city
        logger.debug(f"Запуск подсчетов для city=={city_name} "
                     + f"из процесса {current_process()}")
        daily_averages = data_frame.from_records(
            forecasts,
            index=["average"],
            columns=["average", "day_temp", "clear"],
        ).transpose()

        columns = ["hour", "condition", "temp"]
        types = {"hour": "int32", "temp": "int32"}

        for day in forecasts["forecasts"]:

            # Датафрейм для внутридневных данных.
            hours_day = data_frame.from_records(
                day["hours"], columns=columns
            ).astype(types)
            hours = hours_day.loc[
                operator.and_(
                    hours_day["hour"] >= BOTTOM, hours_day["hour"] < TOP
                )
            ]

            if not hours.empty:
                avg_day_temp = hours["temp"].mean().round(2)

                clearly_hours = hours.query("condition == 'clear'").agg(
                    ["count"]
                )["condition"]["count"]
            else:
                avg_day_temp = None
                clearly_hours = None
            daily_averages.loc["day_temp", day["date"]] = avg_day_temp
            daily_averages.loc["clear", day["date"]] = clearly_hours

        try:
            if daily_averages.empty:
                raise RuntimeError("daily_averages are empty.")
        except RuntimeError as e:
            logger.exception(e)

        # Формируем датафрейм средних и поворачиваем для дальнейшего удобства.
        averages = data_frame(
            daily_averages.mean(axis="columns").round(2).transpose(),
            columns=["total_average"],
        )

        city = data_frame([city_name, None], columns=["city"])

        daily_averages.fillna("Н/Д", inplace=True)
        result = CalculationResultModel(
            city=city, daily_averages=daily_averages, averages=averages
        )

        return result


class DataAggregationTask:
    def __init__(self, filename: str, queue: Union[Queue, None] = None):
        super().__init__()
        self.queue = queue
        self.filename = self._get_filename(filename)

    def _get_filename(self, filename):
        try:
            os.remove(filename)
        except OSError:
            pass
        return filename

    def run(self, source=None):
        if self.queue and not source:
            source = self.queue.get()
        self._aggregate(data=source)

    def _check_empty_file(self):
        return os.path.getsize(self.filename) == 0

    def _aggregate(self, data: CalculationResultModel):
        with open(self.filename, "a+", encoding="utf-8") as file:
            # Подготовка датафреймов с схождению.
            city = data.city.rename(columns={"city": "Город/Дата"})
            daily = data.daily_averages.rename(
                index={"day_temp": "Температура, градусы",
                       "clear": "Без осадков, часы"}
            ).reset_index().rename(columns={"index": ""})
            averages = data.averages.rename(
                columns={"total_average": "Среднее"}).set_axis([0, 1])

            big_data = pd.concat([city, daily, averages], axis=1)

            # Если файл открываем первый раз, то заголовки колонок заполняем.
            if self._check_empty_file():
                big_data.to_csv(file, na_rep="", index=False, encoding="utf-8")
            else:
                big_data.to_csv(file, na_rep="", index=False,
                                header=False, encoding="utf-8")


class DataAnalyzingTask:

    def __init__(self, filename: str):
        self.filename = self._check_filename(filename)
        self._comfortables = list()

    def _check_filename(self, filename: str) -> str:
        try:
            f = open(filename)
        except IOError:
            logger.error(f"Не удается открыть файл {filename}")
        finally:
            f.close()
            return filename

    def run(self):
        cities_list, ratings_list = self.get_ratings()
        try:
            ratings_tmp = data_frame(ratings_list, columns=["Рейтинг"])
        except EmptyDataError:
            logger.exception("Ошибка создания датафрейма из списка рейтингов")

        # Собираем датафрейм с рейтингами для вставки в основной
        ratings_len = len(ratings_tmp) * 2
        ratings_tmp.index = pd.RangeIndex(0, ratings_len, 2)
        ratings_zeros = pd.DataFrame(
            0, index=pd.RangeIndex(1, ratings_len, 2), columns=["Рейтинг"])
        ratings = pd.concat([ratings_tmp, ratings_zeros]).sort_index()
        ratings.replace(0, "", inplace=True)

        # Дополняем файл колонкой рейтингов.
        aggregation = pd.read_csv(self.filename).rename(
            columns=lambda i: "" if i.startswith("Unnamed") else i)
        full = pd.concat([aggregation, ratings], axis=1)
        full.to_csv(self.filename, na_rep="", index=False, encoding="utf-8")

        # Выбираем самый/e удачный/e город/а и возвращаем его/их.
        min_ratings = min(ratings_list)
        best_choices = [idx for idx, rating in enumerate(
            ratings_list) if rating == min_ratings]
        self._comfortables = operator.itemgetter(*best_choices)(cities_list)

    def get_comfortables(self) -> List[str]:
        return self._comfortables

    def get_ratings(self) -> Tuple[List[str], List[int]]:
        # Подготовка датафрейма для анализа
        try:
            df = pd.read_csv(self.filename, usecols=["Город/Дата", "Среднее"])
        except (pd.errors.EmptyDataError, UnicodeDecodeError):
            logger.exception(f"Ошибка открытия файла {self.filename}")
        final_df = pd.concat([
            df[::2]["Среднее"].reset_index(drop=True),
            df[1::2]["Среднее"].reset_index(drop=True)], axis=1)
        cities_list = list(df[::2]["Город/Дата"])
        # Формируем список из кортежей средних температур и ясных дней.
        data_to_rank = list(final_df.itertuples(index=False, name=None))

        # Промежуточный список уникальных (на случай присутствия одинаковых
        # кортежей в исходном) кортежей, сортированный по двум параметрам
        data_sorted = sorted(
            list(set(data_to_rank)),
            key=lambda item: (item[0], item[1]),
            reverse=True
        )
        # Получаем положение элементов исходного списка в отсортированном
        ratings = [data_sorted.index(idx) + 1 for idx in data_to_rank]
        return cities_list, ratings
