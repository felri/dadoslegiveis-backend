from fastapi import FastAPI
from routes import checks, joyplot, treemap, circular_packing, map
from fastapi.middleware.cors import CORSMiddleware

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


@app.get("/")
def read_root():
    return {"Hello": "World"}


app.include_router(treemap.router)

app.include_router(joyplot.router)

app.include_router(checks.router)

app.include_router(map.router)

app.include_router(circular_packing.router)