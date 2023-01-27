from fastapi import APIRouter
import datetime
import db
from scripts.downloader import (
    download_deputies,
)

router = APIRouter()


@router.get("/load_deputies")
def load_deputies():
    download_deputies()
    return {"status": "success"}


@router.get("/get_treemap_data")
def get_treemap_data(start_date: str, end_date: str):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_treemap_data(start_date, end_date)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@router.get("/get_barplot_treemap_block_data")
def get_barplot_treemap_block_data(description: str, start_date: str, end_date: str):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_barplot_treemap_block_data(
            description, start_date, end_date
        )
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@router.get("/get_list_expenses_by_deputy")
def get_list_expenses_by_deputy(
    description: str, start_date: str, end_date: str, name: str
):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_list_expenses_by_deputy(
            description, start_date, end_date, name
        )
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}

