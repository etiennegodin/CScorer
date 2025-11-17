
from abc import ABC, abstractmethod
from ...pipeline import Pipeline

class BaseLoader(ABC):
    @abstractmethod
    async def run(self, pipe:Pipeline):
        """Execute the module and return its output"""
        pass

