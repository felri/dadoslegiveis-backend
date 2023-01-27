from fastapi import APIRouter
import datetime
import db
from scripts.downloader import (
    download_expenses,
    download_expenses_current_year,
    remove_all_files,
    format_csv_data_to_db,
)

router = APIRouter()


@router.get("/get_latest")
def get_latest():
    remove_all_files()
    download_expenses_current_year()
    return {"status": "success"}


@router.get("/create_expenses_table")
def create_expenses_table():
    db.expenses.create_table()
    return {"status": "success"}


@router.get("/download_expenses")
def load_expenses():
    download_expenses()
    return {"status": "success"}


@router.get("/prepare_csvs")
def prepare_csvs():
    format_csv_data_to_db()
    return {"status": "success"}


@router.get("/get_joyplot_data")
def get_joyplot_data(start_date: str, end_date: str, by_party: str):
    try:
        by_party = by_party == "true"
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_joyplot_data(start_date, end_date, by_party)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@router.get("/get_details_by_name_and_day")
def get_details_by_name_and_day(date: str, name: str, by_party: str):
    try:
        by_party = by_party == "true"
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        results = db.expenses.get_details_by_name_and_day(date, name, by_party)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}
