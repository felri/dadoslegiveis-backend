from fastapi import APIRouter
import datetime
import db


router = APIRouter()


@router.get("/get_circular_packing_data")
def get_circular_packing_data(start_date: str, end_date: str):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        results = db.expenses.get_circular_packing_data(start_date, end_date)
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}


@router.get("/get_circular_packing_details")
def get_circular_packing_details(
    start_date: str, end_date: str, name: str
):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        results = db.expenses.get_circular_packing_details(
            name, start_date, end_date
        )
        return results
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}

