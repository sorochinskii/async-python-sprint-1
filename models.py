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
