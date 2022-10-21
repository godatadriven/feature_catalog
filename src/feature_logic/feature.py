from pyspark.sql import Column, functions as sf
from typing import Optional
from dataclasses import dataclass


@dataclass
class Feature:
    name: str
    query: Column
    default: Optional[Column] = None
        
    @property
    def select(self) -> Column:
        query = self.query
        if self.default is not None:
            query = sf.coalesce(query, sf.lit(self.default))
        return query.alias(self.name)