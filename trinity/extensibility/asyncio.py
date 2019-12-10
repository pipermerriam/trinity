from abc import abstractmethod

from asyncio_run_in_process import open_in_process
from async_service import background_asyncio_service
from lahja import EndpointAPI

from trinity._utils.logging import setup_child_process_logging
from trinity._utils.profiling import profiler
from trinity.boot_info import BootInfo

from .component import BaseIsolatedComponent
from .event_bus import EventBusService

import logging
logger = logging.getLogger('trinity')


class AsyncioIsolatedComponent(BaseIsolatedComponent):
    async def run(self) -> None:
        async with open_in_process(self._do_run, self._boot_info) as proc:
            await proc.wait()

    @classmethod
    async def _do_run(cls, boot_info: BootInfo) -> None:
        setup_child_process_logging(boot_info)

        endpoint_name = cls._get_endpoint_name()
        event_bus_service = EventBusService(
            boot_info.trinity_config,
            endpoint_name,
        )
        async with background_asyncio_service(event_bus_service):
            event_bus = await event_bus_service.get_event_bus()

            try:
                if boot_info.profile:
                    with profiler(f'profile_{cls._get_endpoint_name}'):
                        await cls.do_run(boot_info, event_bus)
                else:
                    await cls.do_run(boot_info, event_bus)
            except KeyboardInterrupt:
                return

    @classmethod
    @abstractmethod
    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
