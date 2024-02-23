## Some syntactic suggar on top of nats-micro
##  See https://github.com/charbonnierg/nats-micro
##
## Allow declarative definition of services and endpoints like:
##  @service(name="calc")
##  class CalcService:
##      @endpoint
##      async def add(self, a, b):
##          return a + b
##
##  Services will be instantiated as singleton, as for now.
##
##  Parameters to endpoints will be converted if possible, like
##
##      async def echo_plain(self, req: Request):
##          return req.data()
##
##      async def echo_plain_alt(self, req: Request):
##          await req.respond(req.data())
##
##      async def echo_json(self, data: dict):
##          return data
##
##      async def echo_args(self, a, b):
##          return { 'a': a, 'b': b } 
##    
import contextlib
import inspect
import json

from dataclasses import dataclass
from fastapi.encoders import jsonable_encoder
from nats.aio.client import Client
import micro
from micro import Request

@dataclass
class ServiceDefinition:
    name: str
    version: str
    description: str
    endpoints: dict
    service_cls: object
    service_inst: object

    def __init__(self, name, version, description) -> None:
        self.name = name
        self.version = version
        self.description = description
        self.endpoints = {}
        self.service_inst = None
        self.service_cls = None

    def add_endpoint(self, name, handler):
        endpoint_def = EndpointDefinition(self, name, handler)
        self.endpoints[name] = endpoint_def
        return endpoint_def

    def get_endpoint(self, name):
        return self.endpoints.get(name)

    def resolve_service(self):
        if self.service_inst is None:
            # Instantiate service class as a singleton
            self.service_inst = self.service_cls()

class ServiceRegistry:
    _instance = None

    def __init__(self):
        self.services = {}
        self._current_service = None

    def add_service(self, service: ServiceDefinition):
        self.services[service.name] = service
        self._service_decorator = service
        return service

    def get_service(self, name) -> ServiceDefinition:
        return self.services.get(name)

    @property
    def current_service(self):
        return self._current_service

    def set_current_service(self, service):
        self._current_service = service

    @staticmethod
    def get_registry():
        if ServiceRegistry._instance is None:
            ServiceRegistry._instance = ServiceRegistry()
        return ServiceRegistry._instance

@dataclass
class EndpointDefinition:
    service_def: ServiceDefinition
    name: str
    handler: callable
    _handler_args_spec = None

    def __init__(self, service_def: ServiceDefinition, name: str, handler: callable) -> None:
        self.service_def = service_def
        self.name = name

        if handler is None:
            raise ValueError("handler cannot be None")

        self.handler = handler
        self._inspect_handler(handler)

    def _inspect_handler(self, handler):
        # Introspect the signature of the handler to allow signature-dependent argument conversions
        self._handler_args_spec = inspect.getfullargspec(handler)

    async def _convert_result(self, result, req: Request):
        if result is not None:
            if not isinstance(result, (bytes, bytearray)):
                if not isinstance(result, str):
                    if isinstance(result, (int, float, bool)):
                        result = str(result)
                    else:
                        result = jsonable_encoder(result)

                    # Convert object to json
                    result = json.dumps(result)

                # Convert stringify result to utf-8 bytes
                result = result.encode("utf-8")

            await req.respond(result)

    def _convert_to_args(self, req: Request):
        args = []
        kwargs = {}

        # The signature of the handler
        spec = self._handler_args_spec

        has_self = spec.args and spec.args[0] == 'self'

        # Get name of handler arguments without self, if applicable
        handler_args = spec.args[1:] if has_self else spec.args[:]

        if handler_args and spec.annotations.get(handler_args[0]) == Request:
            args.append(req)
            del handler_args[0]

        if handler_args:
            payload = req.data()
            if len(handler_args) == 1:
                argtype = spec.annotations.get(handler_args[0])

                if argtype in {None, bytes, bytearray}:
                    if argtype in {bytearray}:
                        payload = bytearray(payload)
                elif argtype in {str}:
                    payload = payload.decode("utf-8")
                elif argtype in {dict}:
                    payload = json.loads(payload)

                args.append(payload)
            else:
                kwargs = json.loads(payload.decode("utf-8"))

        return has_self, args, kwargs

    async def message_handler(self, req: Request):
        service_def = self.service_def
        target = service_def.service_inst if service_def.service_inst is not None else service_def.service_cls
        has_self, args, kwargs = self._convert_to_args(req)

        if has_self:
            result = await self.handler(target, *args, **kwargs)
        else:
            result = await self.handler(*args, **kwargs)

        await self._convert_result(result, req)

class ClientBuilder:
    def __init__(self, servers=[]):
        self._servers = servers.split(',') if isinstance(servers, str) else servers
        
        if not self._servers:
            self._servers.append("nats://localhost:4222")
            
    async def run(self, quit_event):
        async with contextlib.AsyncExitStack() as stack:
            nc = Client()
            await nc.connect(self._servers)
            stack.push_async_callback(nc.close)
            await ServiceBuilder().build(stack, nc)
            await quit_event.wait()
        return self

class ServiceBuilder:
    async def build(self, stack, client):
        registry = ServiceRegistry.get_registry()

        for service_def in registry.services.values():
            await self.add_service(stack, client, service_def)

    async def add_service(self, stack, client, service_def):
        service_def.resolve_service()
        svc = await stack.enter_async_context(
            micro.add_service(
                client,
                name=service_def.name,
                version=service_def.version,
                description=service_def.description
            )
        )
        grp = svc.add_group(service_def.name)
        await self.add_endpoints(grp, service_def)
        return svc

    async def add_endpoints(self, grp, service_def):
        for endpoint_def in service_def.endpoints.values():
            await grp.add_endpoint(endpoint_def.name, endpoint_def.message_handler)

def service(name: str, version="1.0.0", description=""):
    registry: ServiceRegistry = ServiceRegistry.get_registry()
    service_def = registry.get_service(name)

    if service_def is None:
        service_def = ServiceDefinition(name=name, version=version, description=description)
        registry.add_service(service_def)

    registry.set_current_service(service_def)

    def decorator(cls):
        service_def.service_cls = cls
        return cls

    return decorator

def endpoint(name=None):
    def decorator(func):
        current_service_def = ServiceRegistry.get_registry().current_service

        if current_service_def is None:
            raise Exception(f"Endpoint {name} has no associated @service decorator")

        endpoint_name = func.__name__ if not name or callable(name) else name

        if current_service_def.get_endpoint(endpoint_name) is not None:
            raise Exception(f"Endpoint {endpoint_name} already exists in service {current_service_def.name}")

        endpoint_def = current_service_def.add_endpoint(endpoint_name, func)
        return endpoint_def.message_handler

    if callable(name):
        return decorator(name)

    return decorator

