"""IOFactory: create Reader/Writer by algorithm name."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .reader import DBReader
from .writer import DBWriter
from ..engine import Reader, Writer


class IOFactory(ABC):
    @abstractmethod
    def create_reader(self, algorithm: str, *, workflow_version: str) -> Reader: ...

    @abstractmethod
    def create_writer(self, algorithm: str) -> Writer: ...


class DBIOFactory(IOFactory):
    def __init__(self, conn_source=None) -> None:
        self._conn_source = conn_source

    def create_reader(self, algorithm: str, *, workflow_version: str) -> Reader:
        return DBReader(algorithm, workflow_version=workflow_version, conn_source=self._conn_source)

    def create_writer(self, algorithm: str) -> Writer:
        return DBWriter(algorithm, conn_source=self._conn_source)