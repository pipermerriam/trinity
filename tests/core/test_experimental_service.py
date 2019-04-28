import asyncio
import logging

import curio

from trinity.experimental import Service


class SimpleService(Service):
    async def start(self) -> None:
        self.logger.info('IN')
        await curio.sleep(0.01)
        self.logger.info('FINISHING')


def test_service_with_curio_sleep():
    service = SimpleService()
    curio.run(service.run)


async def long_sleep():
    await curio.sleep(60)


class WithTimeout(Service):
    async def start(self) -> None:
        task = await curio.spawn(long_sleep())
        try:
            await curio.timeout_after(0.01, task.join())
        except curio.TaskError as err:
            pass
        else:
            assert False


def test_service_with_inner_timeout_warning_does_not_escape(caplog):
    service = WithTimeout()
    with caplog.at_level(logging.DEBUG):
        curio.run(service.run)
        assert 'Traceback' not in caplog.text
    assert service.is_finished
