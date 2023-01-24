from fastapi import FastAPI
from scripts import initialize_db
import db
import datetime
from fastapi.middleware.cors import CORSMiddleware
from datasets.downloader import (
    download_expenses,
    download_deputies,
    download_expenses_current_year,
    remove_all_files,
    format_csv_data_to_db
)

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://192.168.1000:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/check_connection")
def check_connection():
    return {"status": initialize_db.check_connection()}


@app.get("/load_deputies")
def load_deputies():
    remove_all_files()
    download_deputies()
    return {"status": "success"}


@app.get("/get_latest")
def get_latest():
    remove_all_files()
    download_expenses_current_year()
    return {"status": "success"}


@app.get("/download_expenses")
def load_expenses():
    download_expenses()
    return {"status": "success"}


@app.get("/prepare_csvs")
def prepare_csvs():
    format_csv_data_to_db()
    return {"status": "success"}


@app.get("/get_joyplot_data")
def get_joyplot_data(start_date: str, end_date: str, by_party: str):
    try:
        by_party = by_party == "true"
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_joyplot_data(start_date, end_date, by_party)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@app.get("/get_details_by_name_and_day")
def get_details_by_name_and_day(date: str, name: str, by_party: str):
    try:
        by_party = by_party == "true"
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        results = db.expenses.get_details_by_name_and_day(date, name, by_party)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@app.get("/get_treemap_data")
def get_treemap_data(start_date: str, end_date: str):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_treemap_data(start_date, end_date)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@app.get("/get_barplot_treemap_block_data")
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


@app.get("/get_list_expenses_by_deputy")
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

