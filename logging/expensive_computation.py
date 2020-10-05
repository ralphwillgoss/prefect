import logging
from prefect import task
from time import sleep


@task
def expensive_computation(obj: str, data: str) -> str:
    logger = logging.getLogger("lib")
    logger.info(f'Computing {obj} with {data}')
    sleep(1)
    return obj + data