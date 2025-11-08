
from abc import ABC, abstractmethod

class BaseQuery(ABC):
    
    @abstractmethod
    def run(self, config:dict):
        """Execute the module and return its output"""
        pass

