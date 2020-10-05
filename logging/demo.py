import logging
import sys
from time import sleep
from typing import List

import prefect
from prefect import task, Flow, unmapped
from prefect.engine.executors import LocalDaskExecutor

from common_preprocessing import common_preprocessing


@task
def individual_preprocessing(name: str) -> str:
    logger = logging.getLogger("lib")
    logger.info('Preprocessing ' + name)
    sleep(1)
    return name.upper()


@task
def spread_out(data: str) -> List[str]:
    sleep(1)
    return list(data)


@task
def reduce(name, data: List[str]) -> str:
    logger = logging.getLogger("lib")
    logger.info('Reducing ' + name)
    sleep(2)
    return ', '.join(data)


@task
def individual_postprocessing(name, data: str) -> str:
    logger = logging.getLogger("lib")
    logger.info('Postprocessing ' + name)
    sleep(1)
    return f'({data})'


@task
def common_postprocessing(data: List[str]):
    sleep(3)
    logger = logging.getLogger("lib")
    logger.info(', '.join(data))


def create_workflow(n: int, names: str):

    result = []
    with Flow('demo') as flow:
        data = common_preprocessing(n)

        for name in names:
            obj = individual_preprocessing(name)
            exp_comp = expensive_computation.map(unmapped(obj), spread_out(data))
            comp = reduce(name, exp_comp)
            pp = individual_postprocessing(name, comp)
            result.append(pp)

        common_postprocessing(result)

    return flow


def main():
    logger = logging.getLogger("lib")
    logger.setLevel('INFO')
    log_stream = logging.StreamHandler(sys.stdout)
    log_stream.setFormatter("%(funcName)s")
    logger.addHandler(log_stream)

    flow = create_workflow(3, 'abc')
    flow.run(executor=LocalDaskExecutor(scheduler='processes', n_workers=4))


if __name__ == '__main__':
    main()
