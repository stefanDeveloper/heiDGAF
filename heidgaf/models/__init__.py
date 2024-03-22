
from abc import ABCMeta, abstractmethod


class Model(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def train():
        pass
    
    @abstractmethod
    def evaluate():
        pass
