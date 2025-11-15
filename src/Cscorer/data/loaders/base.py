
from abc import ABC, abstractmethod
from ...core import Pipeline

class BaseLoader(ABC):
    @abstractmethod
    async def run(self, pipe:Pipeline):
        """Execute the module and return its output"""
        pass

