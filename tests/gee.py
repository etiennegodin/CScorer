import ee, geemap
import os 
from dotenv import load_dotenv 

load_dotenv()
ee.Authenticate()
ee.Initialize(project=os.getenv("GEE_PROJECT"))


