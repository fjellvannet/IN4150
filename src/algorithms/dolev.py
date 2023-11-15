from ipv8.messaging.payload_dataclass import overwrite_dataclass
from dataclasses import dataclass

from da_types import DistributedAlgorithm, CommunitySettings, message_wrapper, Peer

# We are using a custom dataclass implementation.
dataclass = overwrite_dataclass(dataclass)


@dataclass(
    msg_id=1
)  # The value 1 identifies this message and must be unique per community.
class Message:
    message: str


class DolevProtocol(DistributedAlgorithm):
    """_summary_
    Simple example that just echoes messages between two nodes
    Args:
        DistributedAlgorithm (_type_): _description_
    """

    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(Message, self.on_message)

    def on_start(self):
        # Have 0 broadcast a message.
        if self.node_id == 0:
            for peer in self.nodes:
                self.ez_send(peer, Message("Hello there!"))
            self.delivered = True
            print(f'Node {self.node_id} has got message "Hello there!".')
            self.stop()

    @message_wrapper(Message)
    async def on_message(self, peer: Peer, payload: Message):
        pass
