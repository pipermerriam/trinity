from abc import ABC, abstractmethod
import logging
from typing import (
    Any,
    Type,
)

import curio


class Controller:
    logger = logging.getLogger('trinity.experimental.Controller')

    def __init__(self) -> None:
        self.booted = curio.Event()
        self.started = curio.Event()
        self.cancelled = curio.Event()
        self.cleaned_up = curio.Event()
        self.finished = curio.Event()

        self._run_lock = curio.Lock()

    @property
    def is_running(self) -> bool:
        return self._run_lock.locked()

    async def __aenter__(self) -> None:
        self.logger.debug('START: Entering Controller context')
        group = curio.TaskGroup()
        await self._run_lock.acquire()
        self.logger.debug('Acquired run run lock')
        try:
            await group.__aenter__()
        except Exception:
            self.logger.error('Error entering TaskGroup context')
            await self._run_lock.release()
            raise
        else:
            self.logger.debug('Entered TaskGroup context')

        self.group = group
        self.logger.debug('FINISH: Entering Controller context')

        return self

    async def __aexit__(self, exc_type: Type[Exception], exc: Exception, tb: Any) -> None:
        self.logger.debug('START: Exiting Controller context')
        await self._run_lock.release()
        try:
            self.logger.debug('Waiting for TaskGroup.join()')
            await curio.timeout_after(20, self.group.join())
            self.logger.debug('TaskGroup.join() finished')
        finally:
            await self.group.__aexit__(exc_type, exc, tb)
        del self.group
        self.logger.debug('FINISH: Exiting Controller context')


class Service(ABC):
    logger = logging.getLogger('trinity.experimental.Service')

    controller: Controller

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def set_controller(self, controller: Controller) -> None:
        if hasattr(self, 'controller'):
            raise AttributeError('TODO')
        self.controller = controller

    @abstractmethod
    async def start(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def run(self) -> None:
        run_task = await curio.spawn(self._run, self)
        await run_task.join()

    @classmethod
    async def _run(cls, service: 'Service') -> None:
        """
        Service Lifecycle
        -----------------

        booted -> started --> cancelled -> cleaned_up -> finished
        """
        cls.logger.info('Running service: %s', service.name)
        service = service
        controller = Controller()

        service.set_controller(controller)

        async with controller as controller:
            cls.logger.info('Service[%s]: booted', service.name)
            await controller.booted.set()

            start_task = await controller.group.spawn(service.start())
            cls.logger.info('Service[%s]: started', service.name)
            await controller.started.set()

            try:
                # wait for the service to exit
                cls.logger.info('Service[%s]: joining...', service.name)
                await start_task.join()
                cls.logger.info('Service[%s]: stopped', service.name)
            except Exception:
                cls.logger.error('Service[%s]: errored', service.name)
            finally:
                await controller.cancelled.set()

            cls.logger.info('Service[%s]: Cleaning up...')
            cleanup_task = await controller.group.spawn(service.cleanup())
            cls.logger.info('Service[%s]: cleaned-up', service.name)
            await cleanup_task.join()

        # Finished once the `Controller` context exits.
        cls.logger.info('Service[%s]: finished', service.name)
        await controller.finished.set()


def service(coro: Coroutine[) -> Service:
    class _Service(Service):
        def __init__(
        async def start(self) -> None:
            task = await curio.spawn(coro())
