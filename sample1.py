"""Minimal example of NATS micro usage with decorators."""

import asyncio
import signal
import os
import sys

from pydantic import BaseModel

from nats.aio.client import Client
from hazelnats import ClientBuilder, service, endpoint

from micro import Request

async def main(nats_server):
    # Define an event to signal when to quit
    quit_event = asyncio.Event()

    # Attach signal handler to the event loop
    loop = asyncio.get_event_loop()

    for sig in (signal.Signals.SIGINT, signal.Signals.SIGTERM):
        loop.add_signal_handler(sig, lambda *_: quit_event.set())

    await ClientBuilder(nats_server).run(quit_event)

## Some sample micro services
## There will be one singleton instance of this serivce created, as for now    
@service(name="sample")
class SampleService:
    def __init__(self):
        self._counter = 0

    # request, response pattern, default behaviour
    @endpoint
    async def reply_one(self, req: Request):
        data = req.data()
        await req.respond(data)

    # reply will be handled automatically

    @endpoint
    async def reply_two(self, req: Request):
        return req.data()

    # automatic mapping of arguments from request payload, same for result handling
    @endpoint
    async def say_hello(self, s: str):
        return f"Hello {s}!"
    
    @endpoint
    async def counter(self):
        self._counter += 1
        return { "result": self._counter} 

    @endpoint
    async def get_pid(self):
        return { 'pid': os.getpid()}

@service(name="calc")
class CalcService:
    @endpoint(name = "add")
    async def add(self, a, b):
        return { 'result': a + b }
    
if __name__ == "__main__":
    nats_server = sys.argv[1] if len(sys.argv) > 1 else "nats://localhost:4222"
    asyncio.run(main(nats_server))
