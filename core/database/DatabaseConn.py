from abc import ABC, ABCMeta, abstractmethod

class DatabaseConn(ABC):
    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    def select_as_dataframe(self):
        raise NotImplementedError

    @abstractmethod
    def delete_record(self):
        raise NotImplementedError

    @abstractmethod
    def insert_from_dataframe(self):
        raise NotImplementedError