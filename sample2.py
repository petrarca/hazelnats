## Same sample service, to show how to deal with serialization of (data) objects
##
import asyncio
import signal
import secrets
import sys
from faker import Faker
from pydantic import BaseModel
#from fastapi.encoders import jsonable_encoder

from hazelnats import ClientBuilder, service, endpoint

def generate_pseudo_id(length=16):
    return secrets.token_hex(length // 2)

class Patient(BaseModel):
    id: str
    first_name: str
    last_name: str
    dob: str
    gender: str

@service(name="patients")
class PatientService:
    def __init__(self):
        self._patients = []
        self._fake = Faker()
        self._populate_patients()

    @endpoint
    async def get_all(self):
        #return jsonable_encoder(self._patients)
        return self._patients

    @endpoint
    async def get(self, id: str):
        for p in self._patients:
            if p.id == id:
                return p
            
        return { "result" : f"Patient with id {id} not found." }

    def _populate_patients(self):
        fake = Faker()
        for _ in range(1,100):
            self._patients.append(Patient.parse_obj(self._fake_patient()))
    def _fake_patient(self):
        return {
            "id" : generate_pseudo_id(), 
            "first_name" : self._fake.first_name(), 
            "last_name" : self._fake.last_name(),
            "dob" : self._fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            "gender" : self._fake.random_element(elements=('male', 'female'))
        }
async def main(nats_server):
    # Define an event to signal when to quit
    quit_event = asyncio.Event()

    # Attach signal handler to the event loop
    loop = asyncio.get_event_loop()

    for sig in (signal.Signals.SIGINT, signal.Signals.SIGTERM):
        loop.add_signal_handler(sig, lambda *_: quit_event.set())

    await ClientBuilder(nats_server).run(quit_event)

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
