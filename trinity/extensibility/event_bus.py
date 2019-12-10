import asyncio
from typing import Union, Type

import trio
import sniffio

from lahja import (
    AsyncioEndpoint,
    ConnectionConfig,
    BroadcastConfig,
    TrioEndpoint,
    EndpointAPI,
)

from async_service import Service

from trinity.config import TrinityConfig
from trinity.constants import MAIN_EVENTBUS_ENDPOINT
from trinity.events import AvailableEndpointsUpdated, EventBusConnected


class EventBusService(Service):
    _endpoint_available: Union[trio.Event, asyncio.Event]
    _async_lib_name: str

    def __init__(
        self,
        trinity_config: TrinityConfig,
        endpoint_name: str,
    ) -> None:
        self._trinity_config = trinity_config

        self._async_lib_name = sniffio.current_async_library()
        if self._async_lib_name == 'asyncio':
            self._endpoint_available = asyncio.Event()
        elif self._async_lib_name == 'trio':
            self._endpoint_available = trio.Event()
        else:
            raise TypeError(f"Unsupported async library: {self._async_lib_name}")

        self._connection_config = ConnectionConfig.from_name(
            endpoint_name, self._trinity_config.ipc_dir
        )

    async def get_event_bus(self) -> None:
        await self._endpoint_available.wait()
        return self._endpoint

    async def run(self) -> None:
        endpoint_cls: Type[EndpointAPI]
        if self._async_lib_name == 'asyncio':
            endpoint_cls = AsyncioEndpoint
        elif self._async_lib_name == 'trio':
            endpoint_cls = TrioEndpoint
        else:
            raise TypeError(f"Unsupported async library: {self._async_lib_name}")

        async with endpoint_cls.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint

            # run background task that automatically connects to newly announced endpoints
            self.manager.run_daemon_task(self._auto_connect_new_announced_endpoints)

            # connect to the *main* endpoint which communicates information
            # about other endpoints that come online.
            main_endpoint_config = ConnectionConfig.from_name(
                MAIN_EVENTBUS_ENDPOINT, self._trinity_config.ipc_dir
            )
            await endpoint.connect_to_endpoints(main_endpoint_config)

            # announce ourself to the event bus
            await endpoint.wait_until_endpoint_subscribed_to(
                main_endpoint_config.name,
                EventBusConnected,
            )
            await endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=main_endpoint_config.name)
            )

            # signal that the endpoint is now available.  This is
            # intentionally done after setting up the various subscriptions
            # and daemon tasks above.
            self._endpoint_available.set()

            # wait for external cancellation
            await self.manager.wait_finished()

    async def _auto_connect_new_announced_endpoints(self) -> None:
        """
        Connect the given endpoint to all new endpoints on the given stream
        """
        async for ev in self._endpoint.stream(AvailableEndpointsUpdated):
            yield ev
            # We only connect to Endpoints that appear after our own Endpoint in the set.
            # This ensures that we don't try to connect to an Endpoint while that remote
            # Endpoint also wants to connect to us.
            endpoints_to_connect_to = tuple(
                connection_config
                for index, val in enumerate(ev.available_endpoints)
                if val.name == self._endpoint.name
                for connection_config in ev.available_endpoints[index:]
                if not self._endpoint.is_connected_to(connection_config.name)
            )
            if not endpoints_to_connect_to:
                continue

            endpoint_names = ",".join((config.name for config in endpoints_to_connect_to))
            self.logger.debug(
                "EventBus Endpoint %s connecting to other Endpoints: %s",
                self._endpoint.name,
                endpoint_names,
            )
            try:
                await self._endpoint.connect_to_endpoints(*endpoints_to_connect_to)
            except Exception as e:
                self.logger.warning(
                    "Failed to connect %s to one of %s: %s",
                    self._endpoint.name,
                    endpoint_names,
                    e,
                )
                raise
