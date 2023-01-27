from fastapi import APIRouter
from scripts import initialize_db
from scripts.cache import check_redis_connection

router = APIRouter()


@router.get("/check_db_connection")
def check_connection():
    return {"status": initialize_db.check_connection()}


# check connection with redis
@router.get("/check_redis_connection")
def check_redis():
    return {"status": check_redis_connection()}