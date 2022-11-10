from pandas import DataFrame
from pydantic import BaseModel


class HourModel(BaseModel):
    hour: int
    temp: int
    condition: str


class DateModel(BaseModel):
    date: str
    hours: list[HourModel]


class ForecastsModel(BaseModel):
    forecasts: list[DateModel]


class ForecastsCityModel(BaseModel):
    city: str
    forecasts: ForecastsModel


class CalculationResultModel(BaseModel):
    city: DataFrame
    daily_averages: DataFrame
    averages: DataFrame

    class Config:
        arbitrary_types_allowed = True
