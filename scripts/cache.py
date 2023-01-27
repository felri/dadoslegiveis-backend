import redis
import os
import simplejson as json 

EXPIRATION_TIME = 1 * 86400


def get_redis_instance():
    return redis.Redis(host=os.environ.get("REDIS_HOST"), port=6379, db=0)


def check_redis_connection():
    try:
        r = get_redis_instance()
        r.ping()
        return True
    except:
        return False


def cache_function(function, cache_key):
    """
    A generic function that caches the results of another function
    """
    r = get_redis_instance()
    # Check if the data is already in the cache
    cached_data = r.get(cache_key)
    if cached_data is not None:
        # If the data is in the cache, parse the JSON
        return json.loads(cached_data)

    # If the data is not in the cache, call the function and cache the result
    result = function()
    r.set(cache_key, json.dumps(result))
    r.expire(cache_key, EXPIRATION_TIME)
    return result
