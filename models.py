from typing import List

from pandas import DataFrame
from pydantic import BaseModel


class HourModel(BaseModel):
    hour: int
    temp: int
    condition: str


class DateModel(BaseModel):
    date: str
    hours: List[HourModel]
# TODO С версии python 3.9 для стандартных коллекций больше не нужен
# модуль typing:
# https://docs.python.org/3.9/whatsnew/3.9.html#
# type-hinting-generics-in-standard-collections.
# Поправь во всем проекте


class ForecastsModel(BaseModel):
    forecasts: List[DateModel]


class ForecastsCityModel(BaseModel):
    city: str
    forecasts: ForecastsModel


class ForecastCityListModel(BaseModel):
    city_forecasts: List[ForecastsCityModel]


class CalculationResultModel(BaseModel):
    city: DataFrame
    daily_averages: DataFrame
    averages: DataFrame

    class Config:
        arbitrary_types_allowed = True
