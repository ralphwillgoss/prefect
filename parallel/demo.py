import logging
from time import sleep
from typing import List

from prefect import task, Flow, unmapped
from prefect.engine.executors import LocalDaskExecutor


def common_preprocessing(n: int) -> str:
    sleep(1)
    return ''.join([str(i) for i in range(n)])


@task
def individual_preprocessing(name: str) -> str:
    msg = 'Preprocessing ' + name
    logging.info(msg)
    print(msg)
    sleep(5)
    return name.upper()


@task
def spread_out(data: str) -> List[str]:
    sleep(2)
    return list(data)


@task
def expensive_computation(obj: str, data: str) -> str:
    msg = f'Computing {obj} with {data}'
    logging.info(msg)
    print(msg)
    sleep(5)
    return obj + data


@task
def reduce(name, data: List[str]) -> str:
    msg = 'Reducing ' + name
    logging.info(msg)
    print(msg)
    sleep(2)
    return ', '.join(data)


@task
def individual_postprocessing(name, data: str) -> str:
    msg = 'Postprocessing ' + name
    logging.info(msg)
    print(msg)
    sleep(2)
    return f'({data})'


@task
def common_postprocessing(data: List[str]):
    sleep(2)
    msg = ', '.join(data)
    logging.info(msg)
    print(msg)


def create_workflow(n: int, letters: str):

    data = common_preprocessing(n)
    result = []
    with Flow('demo') as flow:

        for letter in letters:
            # why isn't individual_preprocessing being parallelised?
            obj = individual_preprocessing(letter)
            r = reduce(letter, expensive_computation.map(unmapped(obj), spread_out(data)))
            o = individual_postprocessing(letter, r)
            result.append(o)

        common_postprocessing(result)
    return flow


def main():
    flow = create_workflow(3, 'abc')

    # flow.visualize()
    flow.run(executor=LocalDaskExecutor(scheduler='Processes', n_workers=3))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s ', filename='demo.log')
    main()
