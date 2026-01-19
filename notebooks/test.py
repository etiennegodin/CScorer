from obsq.data.ingest.inat.api import inatObservationApi
import asyncio

id = 298621336

request = asyncio.run(inatObservationApi('id_request', id).request())
