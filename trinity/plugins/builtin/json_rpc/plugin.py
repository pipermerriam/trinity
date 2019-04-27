from abc import ABC, abstractmethod
import logging
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import (
    Any,
    Tuple,
    Type,
)

import curio

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    BeaconAppConfig,
    TrinityConfig
)
from trinity.chains.base import BaseAsyncChain
from trinity.db.eth1.manager import (
    create_db_consumer_manager
)
from trinity.extensibility import (
    BaseIsolatedPlugin,
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.plugins.builtin.light_peer_chain_bridge.light_peer_chain_bridge import (
    EventBusLightPeerChain,
)
from trinity.rpc.main import (
    RPCServer,
)
from trinity.rpc.modules import (
    BaseRPCModule,
    initialize_beacon_modules,
    initialize_eth1_modules,
)
from trinity.rpc.ipc import (
    IPCServer,
)
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)


class Controller:
    logger = logging.getLogger('trinity.experimental.Service')

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
        self.logger.debug('Entering Controller context')
        group = curio.TaskGroup()
        await self._run_lock.acquire()
        self.logger.debug('Acquired run run lock')
        try:
            await group.__aenter__()
            self.logger.debug('Entered TaskGroup context')
        except Exception:
            await self._run_lock.release()
            raise

        self.group = group

        return self

    async def __aexit__(self, exc_type: Type[Exception], exc: Exception, tb: Any) -> None:
        await self._run_lock.release()
        try:
            await curio.timeout_after(20, controller.group.join())
        finally:
            await self.group.__aexit__()
        del self.group


class Service(ABC):
    controller: Controller

    def set_controller(self, controller: Controller) -> None:
        if hasattr(self, 'controller'):
            raise AttributeError('TODO')
        self.controller = controller

    @abstractmethod
    async def start(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    @staticmethod
    async def run(service: 'Service') -> None:
        """
        Service Lifecycle
        -----------------

        booted -> started --> cancelled -> cleaned_up -> finished
        """
        service = service
        controller = Controller()

        service.set_controller(controller)

        async with controller as controller:
            controller.booted.set()

            start_task = await controller.group.spawn(service.start())
            controller.started.set()

            try:
                # wait for the service to exit
                await start_task.join()
            finally:
                controller.cancelled.set()

            cleanup_task = controller.group.spawn(service.cleanup())
            await cleanup_task.join()

        # Finished once the `Controller` context exits.
        controller.finished.set()


class JsonRpcServerPlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "JSON-RPC API"

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        if not self.context.args.disable_rpc:
            self.start()

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-rpc",
            action="store_true",
            help="Disables the JSON-RPC Server",
        )

    def setup_eth1_modules(self, trinity_config: TrinityConfig) -> Tuple[BaseRPCModule, ...]:
        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = trinity_config.get_chain_config()

        chain: BaseAsyncChain

        if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = db_manager.get_headerdb()  # type: ignore
            event_bus_light_peer_chain = EventBusLightPeerChain(self.context.event_bus)
            chain = chain_config.light_chain_class(header_db, peer_chain=event_bus_light_peer_chain)
        elif eth1_app_config.database_mode is Eth1DbMode.FULL:
            db = db_manager.get_db()  # type: ignore
            chain = chain_config.full_chain_class(db)
        else:
            raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")

        return initialize_eth1_modules(chain, self.event_bus)

    def setup_beacon_modules(self) -> Tuple[BaseRPCModule, ...]:

        return initialize_beacon_modules(None, self.event_bus)

    def do_start(self) -> None:

        trinity_config = self.context.trinity_config

        if trinity_config.has_app_config(Eth1AppConfig):
            modules = self.setup_eth1_modules(trinity_config)
        elif trinity_config.has_app_config(BeaconAppConfig):
            modules = self.setup_beacon_modules()
        else:
            raise Exception("Unsupported Node Type")

        rpc = RPCServer(modules, self.context.event_bus)
        ipc_server = IPCServer(rpc, self.context.trinity_config.jsonrpc_ipc_path)

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(exit_with_service_and_endpoint(ipc_server, self.context.event_bus))
        asyncio.ensure_future(ipc_server.run())
        loop.run_forever()
        loop.close()
