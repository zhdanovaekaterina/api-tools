import logging
from time import time


logger = logging.getLogger(__name__)


def timetracking(func):
    """
    Декоратор для трекинга времени выполнения асинхронной функции
    :param func:
    :return:
    """

    async def track_time():
        start_time = time()
        logger.info('---Started working---')

        await func()

        end_time = t.time()
        total_time = round((end_time - start_time), 3)
        logger.info(f'---Finished in {total_time} s.')

    return track_time
