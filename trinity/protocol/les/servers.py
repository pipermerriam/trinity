from typing import (
    Any,
    Dict,
    cast,
)

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import CommandAPI, SessionAPI
from p2p.typing import Payload

from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.servers import (
    BaseIsolatedRequestServer,
    BasePeerRequestHandler,
)
from trinity.protocol.les import commands
from trinity.protocol.les.events import (
    GetBlockHeadersEvent,
)
from trinity.protocol.les.peer import (
    LESProxyPeer,
)

from trinity.protocol.les.requests import HeaderRequest as LightHeaderRequest


class LESPeerRequestHandler(BasePeerRequestHandler):
    async def handle_get_block_headers(self, peer: LESProxyPeer, msg: Dict[str, Any]) -> None:

        self.logger.debug("Peer %s made header request: %s", peer, msg)
        request = LightHeaderRequest(
            msg['query'].block_number_or_hash,
            msg['query'].max_headers,
            msg['query'].skip,
            msg['query'].reverse,
            msg['request_id'],
        )
        headers = await self.lookup_headers(request)
        self.logger.debug2("Replying to %s with %d headers", peer, len(headers))
        peer.sub_proto.send_block_headers(headers, buffer_value=0, request_id=request.request_id)


class LightRequestServer(BaseIsolatedRequestServer):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """

    def __init__(
            self,
            event_bus: EndpointAPI,
            broadcast_config: BroadcastConfig,
            db: BaseAsyncHeaderDB) -> None:
        super().__init__(
            event_bus,
            broadcast_config,
            (GetBlockHeadersEvent,),
        )
        self._handler = LESPeerRequestHandler(db)

    async def _handle_msg(self,
                          session: SessionAPI,
                          cmd: CommandAPI,
                          msg: Payload) -> None:

        self.logger.debug2("Peer %s requested %s", session, cmd)
        peer = LESProxyPeer.from_session(session, self.event_bus, self.broadcast_config)
        if isinstance(cmd, commands.GetBlockHeaders):
            await self._handler.handle_get_block_headers(peer, cast(Dict[str, Any], msg))
        else:
            self.logger.debug("%s msg not handled yet, needs to be implemented", cmd)
