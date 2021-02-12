# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 15:00:31 2021

@author: Tony_Tien
"""

from datetime import datetime, timedelta


def transform_Yweek2Date(yweek):
    """
    轉換 yweek 成該周"星期一"日期
    ex: date: '2019-28' --> '2019-07-08'
              '2019-01' --> '2018-12-31'
    """
    year = yweek.split("-")[0]
    week = yweek.split("-")[1]

    year_start_date = datetime.strptime(year + "-01-01", "%Y-%m-%d")
    year_start_calendar_msg = year_start_date.isocalendar()

    year_start_year = year_start_calendar_msg[0]
    year_start_week = year_start_calendar_msg[1]
    year_start_weekday = year_start_calendar_msg[2]

    if year_start_year < int(year):
        daydelat = (8 - int(year_start_weekday)) + (int(week) - 1) * 7
    else:
        daydelat = (8 - int(year_start_weekday)) + (int(week) - 2) * 7

    date = (year_start_date + timedelta(days=daydelat)).date()
    date = datetime.strftime(date, "%Y-%m-%d")
    return date


def transform_Date2Yweek(date):
    """
    ex: date: '2019-10-22' --> '2019-43'
    """
    now = datetime.strptime(date, "%Y-%m-%d").isocalendar()
    return str(now[0]).zfill(4) + "-" + str(now[1]).zfill(2)


def transform_Date2WeekDay(date):
    """
    轉換 日期 成星期幾
    ex: date: '2020-01-01' --> '3'
        date: '2020-01-05' --> '7'
    """
    datetime_date = datetime.strptime(date, "%Y-%m-%d")
    return str(datetime_date.weekday() + 1)


def add_Weekdelta2Yweek(yweek, week_nbr):
    """
    ex: date: yweek:'2019-15', weeb_nbr:5 --> '2019-20'
    """
    date = transform_Yweek2Date(yweek)
    date = datetime.strptime(date, "%Y-%m-%d")
    date += timedelta(days=7 * (week_nbr))
    date = date.isocalendar()
    return str(date[0]).zfill(4) + "-" + str(date[1]).zfill(2)


def add_Daydelta2Date(date, days):
    """
    ex: date: yweek:'2020-01-01', days:5 --> '2020-01-06'
        date: yweek:'2020-01-01', days:-5 --> '2019-12-27'
    """
    date = datetime.strptime(date, "%Y-%m-%d")
    date += timedelta(days=days)
    return datetime.strftime(date, "%Y-%m-%d")


def calculate_Weeknbr(yweek1, yweek2):
    assert yweek1 <= yweek2, "yweek2 必須大於等於 yweek1"

    date_early = transform_Yweek2Date(yweek1)
    date_late = transform_Yweek2Date(yweek2)

    dis_day = datetime.strptime(date_late, "%Y-%m-%d") - datetime.strptime(
        date_early, "%Y-%m-%d"
    )
    return int(dis_day.days / 7)


def calculate_WeekOfMonth(date):
    """
    ex: date: 2020-12-01 --> 1
    ex: date: 2020-12-28 --> 5
    ex: date: 2019-12-30 --> 6
    """
    date = datetime.strptime(date, "%Y-%m-%d")

    nom = date.isocalendar()[1] - date.replace(day=1).isocalendar()[1] + 1
    return nom + 52 if nom < 0 else nom


def get_Current_Date():
    date = datetime.now().strftime("%Y-%m-%d")
    return date


def get_Current_YWeek():
    current_date = get_Current_Date()
    yweek = transform_Date2Yweek(current_date)
    return yweek


def get_Current_Month():
    curr_month = datetime.today().month
    return curr_month
