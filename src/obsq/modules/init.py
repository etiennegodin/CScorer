from ..pipeline import Module
from ..steps. database import DataBaseConnection

init = Module('init', [DataBaseConnection])