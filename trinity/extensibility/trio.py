from abc import abstractmethod
import asyncio
import logging
import signal

import trio
from async_service import background_trio_service

from lahja import EndpointAPI

from trinity._utils.ipc import kill_process_gracefully
from trinity._utils.logging import setup_child_process_logging
from trinity._utils.mp import ctx
from trinity._utils.os import friendly_filename_or_url
from trinity._utils.profiling import profiler
from trinity.boot_info import BootInfo

from .component import BaseIsolatedComponent
from .event_bus import EventBusService


class TrioIsolatedComponent(BaseIsolatedComponent):
    async def run(self) -> None:
        """
        Call chain is:

        - multiprocessing.Process -> _run_process
            * isolates to a new process
        - _run_process -> run_process
            * sets up subprocess logging
        - run_process -> _do_run
            * runs the event loop and transitions into async context
        - _do_run -> do_run
            * sets up event bus and then enters user function.
        """
        process = ctx.Process(
            target=self.run_process,
            args=(self._boot_info,),
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, process.start)
        try:
            await loop.run_in_executor(None, process.join)
        finally:
            kill_process_gracefully(
                process,
                logging.getLogger('trinity.extensibility.TrioIsolatedComponent'),
            )

    @classmethod
    def run_process(cls, boot_info: BootInfo) -> None:
        setup_child_process_logging(boot_info)
        if boot_info.profile:
            with profiler(f'profile_{cls._get_endpoint_name}'):
                trio.run(cls._do_run, boot_info)
        else:
            trio.run(cls._do_run, boot_info)

    @classmethod
    async def _do_run(cls, boot_info: BootInfo) -> None:
        if cls.endpoint_name is None:
            endpoint_name = friendly_filename_or_url(cls.name)
        else:
            endpoint_name = cls.endpoint_name
        event_bus_service = EventBusService(
            boot_info.trinity_config,
            endpoint_name,
        )
        with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signal_aiter:
            async with background_trio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(cls.do_run, boot_info, event_bus)
                    async for sig in signal_aiter:
                        nursery.cancel_scope.cancel()

    @classmethod
    @abstractmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        This is where subclasses should override
        """
        ...
