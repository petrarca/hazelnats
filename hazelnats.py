import contextlib
import inspect
import json

from dataclasses import dataclass
from nats.aio.client import Client
import micro
from micro import Request

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
        if self.service_inst is not None:
            return
        
        # AS for now instanciate service class as singleton
        self.service_inst = self.service_cls()
    
class ServiceRegistry:
    def __init__(self):
        self.services = {}
        self._current_service = None

    def add_service(self, service : ServiceDefinition):
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

    # Singleton of the service registry    
    _instance = None

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
        # introspect the signature of the handler to allow signature dependent argument converations
        self._handler_args_spec = inspect.getfullargspec(handler)

    async def _convert_result(self, result, req: Request) :
        if result is None:
            return
        
        if not isinstance(result,(bytes,bytearray)):
            if not isinstance(result, str):
                if isinstance(result,(int, float, bool)):
                   result = str(result)
                else:
                    # convert object to json
                    result = json.dumps(result)

            # convert stringify result to utf-8 bytes
            result = result.encode("utf-8")

        await req.respond(result)

    def _convert_to_args(self, req: Request):
        args = []
        kwargs = {}

        # the signature of the handler
        spec = self._handler_args_spec

        has_self = False
        if len(spec.args) > 0 and spec.args[0] == 'self':
            has_self = True

        # get name of handler arguments, without self, if applicable
        handler_args = spec.args[1:] if has_self else spec.args[:]        

        if len(handler_args) > 0:
            # handle signature handler([self,] req: Request[,])
            if spec.annotations.get(handler_args[0]) == Request:
                args.append(req)
            
                del handler_args[0]

        # process remaining handler arguments, without optional request parameter
        if len(handler_args) > 0:
            payload = req.data()

            # handle signature handler(..., arg: <type>)
            if len(handler_args) == 1:
                # handle typed argument
                argtype = spec.annotations.get(handler_args[0])

                # convert payload depending on argument type
                if argtype in { None, bytes, bytearray }:
                    if argtype in { bytearray }:
                        payload = bytearray(payload)
                elif argtype in { str }:
                    payload = payload.decode("utf-8")
                elif argtype in { dict } :
                    payload = json.loads(payload)
                
                args.append(payload)
            else:
                # convert to json, pass as kwargs to handler
                kwargs = json.loads(payload.decode("utf-8"))
        else:
            # no arguments provided, assume no request parameter
            pass

        return has_self, args, kwargs      

    async def message_handler(self, req: Request):
        service_def = self.service_def

        # instance if singleton, else assume handler is class method
        target = service_def.service_inst if service_def.service_inst is not None else service_def.service_cls

        has_self, args, kwargs = self._convert_to_args(req)

        # delegate to the endpoint implementation
        if has_self:
            result = await self.handler(target, *args, **kwargs)
        else:
            result = await self.handler(*args, **kwargs)
        
        await self._convert_result(result, req)

class ClientBuilder:
    def __init__(self,servers = ["nats://localhost:4222"]):
        if isinstance(servers, str):
            self._servers = servers.split(',')
        else:
            self._servers = servers

    async def run(self, quit_event):
        async with contextlib.AsyncExitStack() as stack:
            # Create a NATS client
            nc = Client()

            # Connect to NATS
            await nc.connect(self._servers)

            # Push the client.close() method into the stack to be called on exit
            stack.push_async_callback(nc.close)

            # Push decorated micro services into the stack to be stopped on exit
            # The service will be stopped and drain its subscriptions before
            # closing the connection.
            await ServiceBuilder().build(stack, nc)

            # Wait for the quit event
            await quit_event.wait()

        return self

class ServiceBuilder:
    async def build(self, stack, client):
        registry  = ServiceRegistry.get_registry()

        # iterate over all services
        for service_def in registry.services.values():
            await self.add_service(stack, client, service_def)

    async def add_service(self, stack, client, service_def):
        # Create instance of class which implemente the service if
        service_def.resolve_service();

        svc = await stack.enter_async_context(
            micro.add_service(
                client,
                name=service_def.name,
                version=service_def.version,
                description=service_def.description
            )
        )

        # Define for now a default group
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

    # Mark this service as current, so @endpoint can reference to the service
    registry.set_current_service(service_def)

    def decorator(cls):
        service_def.service_cls = cls
        return cls

    return decorator

def endpoint(name = None):        
    def decorator(func):
        # Endpoint decorator must be defined within a @service decorator
        current_service_def = ServiceRegistry.get_registry().current_service

        if current_service_def is None:
            raise Exception(f"Endpoint {name} has no associated @service dectorator")
        
        endpoint_name = func.__name__ if not name or callable(name) else name

        if current_service_def.get_endpoint(endpoint_name) is not None:
            raise Exception(f"Endpoint {endpoint_name} already exists in service {current_service_def.name}")
        
        # Add the endpoint to the current service definition
        endpoint_def = current_service_def.add_endpoint(endpoint_name, func)

        return endpoint_def.message_handler
    
    if callable(name):
        return decorator(name)
    
    return decorator
