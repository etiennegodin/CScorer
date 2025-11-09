
from abc import ABC, abstractmethod
from ..core import PipelineData

class BaseQuery(ABC):
    @abstractmethod
    def run(self, data:PipelineData, step_name:str):
        """Execute the module and return its output"""
        pass

