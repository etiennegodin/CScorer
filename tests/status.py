from enum import Enum



class QueryStatus(str, Enum):
    init = 0    
    requested = 1
    pending = 2
    downloaded = 3
    unziped = 4
    completed = 5
    
def func():
    return QueryStatus.downloaded  
    
x = QueryStatus.init

print(x)

x = func()
print(x.__dict__)
print(x._value_)