from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, desc, collect_list,struct

from bestseller.config.config_bs_reader import ConfigBestSeller

@dataclass
class BestSellerBaseModel(A8C):
    """common config reader"""
    spark: SparkSession
    tenant: str

    @abstractmethod
    def transform(self, df: DataFrame, join_id: str) -> DataFrame:
        """builds"""
        pass
    @abstractmethod
    def post_processing(self, df: DataFrame, config: ConfigßestSeller) -> DataFrame:
        """post processing"""
        pass