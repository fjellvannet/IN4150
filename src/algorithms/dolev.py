import asyncio
import json
from multiprocessing import Process
import random
import time
import yaml

from ipv8.messaging.payload_dataclass import overwrite_dataclass
from dataclasses import dataclass

from da_types import DistributedAlgorithm, CommunitySettings, message_wrapper, Peer

# We are using a custom dataclass implementation.
dataclass = overwrite_dataclass(dataclass)


@dataclass(msg_id=1)  # The value 1 identifies this message and must be unique per community.
class Message:
    message: str
    # I wanted Tuple[Int] as a type here. According to
    # https://py-ipv8.readthedocs.io/en/latest/reference/serialization.html that should be possible, however it always
    # returned errors, which I only managed to resolve using json serialization and deserialization.
    sender: int
    path: str = '"[]"'


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
        self.max_fault = 2
        self.md = False

    async def on_start(self):
        # read what to send
        with open("messages/dolev.yaml", "r") as f:
            msgs = yaml.safe_load(f)
            for msg in msgs.get(self.node_id, ()):
                p = Process(
                    target=self.dolev_broadcast,
                    args=(
                        Message(msg["message"], msg.get("sender", self.node_id), json.dumps(msg.get("path", ()))),
                        msg["timeout"],
                    ),
                )
                p.start()

    def dolev_broadcast(self, msg: Message, timeout: int):
        time.sleep(timeout)
        self.status(f"Broadcasting {msg.message}")
        self.dolev_deliver(msg)
        asyncio.run(self.send(msg))

    async def send(self, payload: Message):
        path = json.loads(payload.path)
        for node_id, peer in filter(lambda x: x[0] not in path, self.nodes.items()):
            await asyncio.sleep(random.uniform(1.0, 3.0))
            self.status(f"Sending to {node_id}", f"{payload.message} {payload.path}")
            self.ez_send(peer, payload)

    def dolev_deliver(self, payload: Message):
        if (payload.sender, payload.message) not in self.delivered:
            self.status(
                f"\033[1;32;48mDelivered from Node {payload.sender}",
                f"{payload.message} {self.paths.pop((payload.sender, payload.message), set())}",
            )
            self.delivered.add((payload.sender, payload.message))

    @message_wrapper(Message)
    async def on_message(self, peer: Peer, payload: Message):
        peer.id = self.node_id_from_peer(peer)
        path = (*json.loads(payload.path), peer.id)
        if self.md and len(path) == 1 and peer.id == payload.sender:
            self.status(f"Received message directly from node {peer.id}", f"{payload.message}")
            await self.send(Message(payload.message, json.dumps(path)))
            self.dolev_deliver(payload)
        elif len(set(path)) == len(path):  # there are no duplicate nodes in the path
            self.status(
                f"Received message from node {peer.id}",
                f"{payload.message} {payload.path}",
            )
            path_key = (payload.sender, payload.message)
            if path_key in self.paths:
                self.paths[path_key].add(path)
                # the message from node path[0] has been received from at least f + 1 distinct paths
                if len(self.paths.get(path_key, ())) >= self.max_fault + 1:
                    await self.send(Message(payload.message, payload.sender, json.dumps(path)))
                    self.dolev_deliver(payload)
                else:
                    await self.send(Message(payload.message, payload.sender, json.dumps(path)))
            else:
                self.paths[path_key] = {path}
                await self.send(Message(payload.message, payload.sender, json.dumps(path)))
        else:
            self.status(
                f'\033[1;31;48mReceived erroneous message "{payload.message}" with duplicate nodes in path',
                str(path),
            )

    def status(self, description: str, content: str = ""):
        print(f"[Node {self.node_id}] {description}{f': {content}' if content else ''}")
