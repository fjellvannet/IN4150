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
        self.message_info = {}
        self.delivered = set()
        self.max_fault = 1
        self.md = True
        self.trusted_neighbours = set()

    async def on_start(self):
        # read what to send
        with open("config/dolev.yaml", "r") as f:
            node_configs = yaml.safe_load(f)
            node = node_configs.get(self.node_id, {'messages': ()})
            for msg in node.get("messages", ()):
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
        path_key = (payload.sender, hash(payload.message))

        # Send to all neighbours that are not the sender, and have not delivered
        for node_id, peer in filter(lambda x: x[0] not in path and x[0] not in self.message_info.get(path_key, {'neighbours':()})["neighbours"], self.nodes.items()):
            await asyncio.sleep(random.uniform(1.0, 3.0))
            self.status(f"Sending to {node_id}", f"{payload.message} {payload.path}")
            self.ez_send(peer, payload)

    def dolev_deliver(self, payload: Message):
        path_key = (payload.sender, hash(payload.message))
        if path_key not in self.delivered:
            self.status(
                f"\033[1;32;48mDelivered from Node {payload.sender}",
                f"{payload.message} {self.message_info.pop((payload.sender, payload.message), set())}",
            )
            self.delivered.add(path_key)

    @message_wrapper(Message)
    async def on_message(self, peer: Peer, payload: Message):
        peer.id = self.node_id_from_peer(peer)
        path = (*json.loads(payload.path), peer.id)

        if set(path).intersection(self.trusted_neighbours):
            # MD 4 <-- not 100% on this
            return

        if len(set(path)) != len(path):
            # Path contains duplicate(s)
            self.status(
                f'\033[1;31;48mReceived erroneous message "{payload.message}" with duplicate nodes in path',
                str(path),
            )
            return

        self.status(f"Received message from node {peer.id}", f"{payload.message} {payload.path}")
        path_key = (payload.sender, hash(payload.message))

        if self.md and path_key in self.delivered:
            # MD5
            return


        if path_key in self.message_info:
            self.disjoint_add(path_key, path)

            if self.md and len(path) == 1:
                # Add neighbour to the no send list
                self.message_info[path_key]["neighbours"].add(peer.id)

                if peer.id == payload.sender:
                    # Message was recieved directly
                    self.status(f"Received message directly from node {peer.id}", f"{payload.message}")
                    self.dolev_deliver(payload)
                    await self.send(Message(payload.message, payload.sender))
                    return

            # the message from node path[0] has been received from at least f + 1 distinct paths
            if len(self.message_info[path_key]) >= self.max_fault + 1:
                self.dolev_deliver(payload)
                await self.send(Message(payload.message, payload.sender, json.dumps(() if self.md else path)))
            else:
                await self.send(Message(payload.message, payload.sender, json.dumps(path)))
        else:
            self.message_info[path_key] = {"paths": {path}, "neighbours": set()}
            await self.send(Message(payload.message, payload.sender, json.dumps(path)))

    def disjoint_add(self, key, path):
        s_path = set(path)
        for dp in self.message_info[key]['paths']:
            s_dp = set(dp)
            if s_dp.intersection(s_path):
                # path not disjoint
                if len(s_path) < len(s_dp):
                    self.message_info[key]['paths'].remove(dp)
                    self.message_info[key]['paths'].add(path)
                return
        self.message_info[key]['paths'].add(path)


    def status(self, description: str, content: str = ""):
        print(f"[Node {self.node_id}] {description}{f': {content}' if content else ''}")
