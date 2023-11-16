import asyncio
import json
import random
from typing import Tuple

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
        self.max_fault = 3

    async def on_start(self):
        # Have 0 broadcast a message.
        if self.node_id == 0:
            message = Message("Hello there!")
            self.status("Broadcasting", message.message)
            self.deliver(self.node_id, message)
            await self.broadcast(message)

    async def broadcast(self, payload: Message):
        for node_id, peer in self.nodes.items():
            await asyncio.sleep(random.uniform(1.0, 3.0))
            self.status(f"Sending to {node_id}", f"{payload.message} {payload.path}")
            self.ez_send(peer, payload)

    def deliver(self, sender_id: int, payload: Message):
        if (sender_id, payload.message) not in self.delivered:
            self.status(f"Delivered from Node {sender_id}", payload.message)
            self.delivered.add((sender_id, payload.message))

    @message_wrapper(Message)
    async def on_message(self, peer: Peer, payload: Message):
        peer.id = self.node_id_from_peer(peer)
        path = (*json.loads(payload.path), peer.id)
        if len(set(path)) == len(path):  # there are no duplicate nodes in the path
            path_key = (path[0], payload.message)
            if path_key in self.paths:
                self.paths[path_key].add(path)
                if len(self.paths[path_key]) < self.max_fault + 1:
                    await self.broadcast(Message(payload.message, json.dumps(path)))
                else:  # the message from node path[0] has been received from at least f + 1 distinct paths
                    self.deliver(path[0], payload)
                    del self.paths[path_key]
            else:
                self.paths[path_key] = {(path,)}
                await self.broadcast(Message(payload.message, json.dumps(path)))
        else:
            self.status(f"Received erroneous message \"{payload.message}\" with duplicate nodes in path", str(path))

    def status(self, description: str, content: str = ""):
        print(f"[Node {self.node_id}] {description}{f': {content}' if content else ''}")
