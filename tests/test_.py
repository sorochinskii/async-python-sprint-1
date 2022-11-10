import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pandas as pd
import pytest
from csv_diff import compare, load_csv
from pydantic import parse_obj_as

from models import CalculationResultModel, ForecastsCityModel, ForecastsModel
from tasks import DataAggregationTask, DataCalculationTask, DataFetchingTask

CITIES = ["MOSCOW", "CAIRO", "NOVOSIBIRSK", "BUCHAREST"]


class TestForecasts:
    path = "tests/data/"

    @pytest.fixture(scope="class")
    def fixtured_city(self):
        city = pd.read_csv(f"{self.path}city_moscow.csv")
        daily_averages = pd.read_csv(
            f"{self.path}daily_averages_moscow.csv", index_col=0)
        daily_averages.columns.name = "average"
        averages = pd.read_csv(
            f"{self.path}averages_moscow.csv", index_col=0
        )
        city_data = CalculationResultModel(
            city=city,
            daily_averages=daily_averages,
            averages=averages
        )
        return city_data

    def read_test_file(self):
        with open(f"{self.path}test.json", "r", encoding="utf-8") as file:
            data = json.load(file)
        return data

    @pytest.fixture(scope="class")
    def data_from_test_file(self) -> List[ForecastsCityModel]:
        data = self.read_test_file()
        result = list()
        for city, item in zip(CITIES, data):
            data = parse_obj_as(ForecastsModel, item)
            result.append(ForecastsCityModel(city=city, forecasts=data))
        return result

    def test_fetch(self, data_from_test_file):
        data = data_from_test_file
        get_data = DataFetchingTask()

        with ThreadPoolExecutor() as executor:
            fetched = list(executor.map(get_data.fetch, CITIES))

        assert data == fetched

    def test_calc(self, fixtured_city):
        fixtured_data = fixtured_city
        calculator = DataCalculationTask()
        city = "MOSCOW"
        fetched = DataFetchingTask().fetch(city)

        calculated = calculator.run(fetched)

        assert pd.testing.assert_frame_equal(
            calculated.city, fixtured_data.city) is None
        assert pd.testing.assert_frame_equal(
            calculated.daily_averages, fixtured_data.daily_averages) is None
        assert pd.testing.assert_frame_equal(
            calculated.averages, fixtured_data.averages) is None

    def test_aggregation(self):
        testing_file = self.path + "reference.csv"
        file_to_test = self.path + "test.csv"
        try:
            os.remove(file_to_test)
        except OSError:
            pass
        calculation_task = DataCalculationTask()
        aggregator = DataAggregationTask(file_to_test)
        for city in CITIES:
            fetched = DataFetchingTask().fetch(city)
            calculates = calculation_task.run(data=fetched)
            aggregator.run(source=calculates)

        diff = compare(
            load_csv(open(testing_file, "r"), key="Город/Дата"),
            load_csv(open(file_to_test, "r"), key="Город/Дата"))

        assert diff == {'added': [], 'removed': [], 'changed': [],
                        'columns_added': [], 'columns_removed': []}
