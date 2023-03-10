from fastapi import FastAPI
from routes import checks, joyplot, treemap, circular_packing, map
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from scripts import downloader
import pandas as pd
from db import updater
import os

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://dadoslegiveis.lol",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
@repeat_every(seconds=86400)  # 24 hours
def update_latest_entries_in_csv() -> None:
    downloader.download_expenses_current_year()
    downloader.format_csv_data_to_db()

    current_date = pd.to_datetime("today").date()
    updater.delete_entries_after_date(current_date)
    latest_date = updater.get_latest_date()
    df = updater.get_df_from_csv("./datasets/expenses/")
    # Convert string dates to datetime.date objects
    df["datEmissao"] = pd.to_datetime(
        df["datEmissao"], format="%Y-%m-%dT%H:%M:%S"
    ).dt.date
    # remove all the entries with datEmissao after the current date
    df = df[df["datEmissao"] <= current_date]
    print(f"{len(df)} entries in CSV.")
    print(f"current date: {current_date}")
    print(f"latest date: {latest_date}")
    # Filter the rows with datEmissao after the latest date
    df = df[df["datEmissao"] > latest_date]
    updater.save_to_db(df)
    print(f"Updated {len(df)} entries.")


@app.get("/")
def read_root():
    return {"Hello": "World"}


app.include_router(treemap.router)

app.include_router(joyplot.router)

app.include_router(checks.router)

app.include_router(map.router)

app.include_router(circular_packing.router)
