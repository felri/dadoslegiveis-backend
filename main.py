from fastapi import FastAPI
from routes import checks, joyplot, treemap

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


app.include_router(treemap.router)

app.include_router(joyplot.router)

app.include_router(checks.router)