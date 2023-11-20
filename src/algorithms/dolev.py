import asyncio
import json
import random

from ipv8.messaging.payload_dataclass import overwrite_dataclass
from dataclasses import dataclass, field

from da_types import DistributedAlgorithm, CommunitySettings, message_wrapper, Peer

# We are using a custom dataclass implementation.
dataclass = overwrite_dataclass(dataclass)


@dataclass(
    msg_id=1
)  # The value 1 identifies this message and must be unique per community.
class Message:
    message: str
    # I wanted Tuple[Int] as a type here. According to
    # https://py-ipv8.readthedocs.io/en/latest/reference/serialization.html that should be possible, however it always
    # returned errors, which I only managed to resolve using json serialization and deserialization.
    path: str = "\"[]\""


class DolevProtocol(DistributedAlgorithm):
    """_summary_
    Simple example that just echoes messages between two nodes
    Args:
        DistributedAlgorithm (_type_): _description_
    """

    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(Message, self.on_message)
        self.paths = {}
        self.delivered = set()
        self.trusted_neighbours = set()
        self.max_fault = 1

    async def on_start(self):
        # Have 0 broadcast a message.
        if self.node_id == 0:
            message = Message("Hello there!")
            self.status("Broadcasting", message.message)
            self.deliver(message)
            await self.broadcast(message)

    async def broadcast(self, payload: Message):
        path = json.loads(payload.path)
        for node_id, peer in filter(lambda x: x[0] not in path, self.nodes.items()):
            await asyncio.sleep(random.uniform(1.0, 3.0))
            self.status(f"Sending to {node_id}", f"{payload.message} {payload.path}")
            self.ez_send(peer, payload)

    def deliver(self, payload: Message):
        if payload.message not in self.delivered:
            self.status(
                f"\033[1;32;48mDelivered",
                f"{payload.message} {self.paths.pop(payload.message, set())}"
            )
            self.delivered.add(payload.message)

    @message_wrapper(Message)
    async def on_message(self, peer: Peer, payload: Message):
        peer.id = self.node_id_from_peer(peer)
        path = (*json.loads(payload.path), peer.id)
        if payload.message in self.delivered:
            self.status("Received duplicate message that already has been delivered.", f"{payload.message}")
        elif len(path) == 1:
            self.trusted_neighbours.add(peer.id)
            self.status(f"Received message directly from node {peer.id}", f"{payload.message}")
            self.deliver(payload)
            await self.broadcast(Message(payload.message))
        elif set(path).intersection(self.trusted_neighbours):
            self.status(f"Discarded message containing trusted neighbour in path", f"{payload.path}")
        elif len(set(path)) == len(path):  # there are no duplicate nodes in the path
            self.status(f"Received message from node {peer.id}", f"{payload.message} {payload.path}")
            if payload.message in self.paths:
                self.paths[payload.message].add(path)
                # the message from node path[0] has been received from at least f + 1 distinct paths
                if len(self.paths.get(payload.message, ())) >= self.max_fault + 1:
                    self.deliver(payload)
                    await self.broadcast(Message(payload.message))
                else:
                    await self.broadcast(Message(payload.message, json.dumps(path)))
            else:
                self.paths[payload.message] = {path}
                await self.broadcast(Message(payload.message, json.dumps(path)))
        else:
            self.status(
                f"\033[1;31;48mReceived erroneous message \"{payload.message}\" with duplicate nodes in path", str(path)
            )

    def status(self, description: str, content: str = ""):
        print(f"[Node {self.node_id}] {description}{f': {content}' if content else ''}")
