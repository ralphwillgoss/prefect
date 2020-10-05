from time import sleep
from prefect import task


@task
def common_preprocessing(n: int) -> str:
    sleep(1)
    return ''.join([str(i) for i in range(n)])