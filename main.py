from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import checks, joyplot, treemap

app = FastAPI()

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


app.include_router(treemap.router)

app.include_router(joyplot.router)

app.include_router(checks.router)