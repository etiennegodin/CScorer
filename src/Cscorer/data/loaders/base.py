
from abc import ABC, abstractmethod
from ...core import PipelineData

class BaseLoader(ABC):
    @abstractmethod
    async def run(self, data:PipelineData):
        """Execute the module and return its output"""
        pass

