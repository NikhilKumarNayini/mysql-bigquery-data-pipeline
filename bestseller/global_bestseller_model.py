import os
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from common.data_util.data_writer import Datawriter
from common.db.cosmos.cosmos_util import Cosmosutil
from common.util.dataframe_util import DFFilter
from common.util.strategy_util import StrategyUtil
from common.util.tenant_util import TENANT
from bestseller.bestseller_base_model import BestSellerBaseModel
from bestseller.config.config_bs_reader import ConfigBestSeller, BEST_SELLER_TYPE

@dataclass
class GlobalBestSellerModel (BestSellerBaseModel):
    """common config reader"""
    spark: SparkSession
    tenant: TENANT

    def transform(self, df: DataFrame, join_id):
        df.cache()
        sku_pop_data = df.groupBy(join_id).agg(countDistinct("order_id"). alias("pop")).cache()
        df_transformed = df.join(sku_pop_data, join_id, "left"). na.fill(value=0)
        df_transformed.cache()
        df.unpersist()
        return df_transformed

    def post_processing(self, df: DataFrame, config: ConfigBestSeller):
        if config.properties.processing_filter:
        cat_filters = DFFilter(df)
        df = cat_filters \
            .apply_filter(config.properties.processing_filter.filters) \
            .apply_select(config.properties.processing_filter.select) \
            .apply_rename(config. properties.processing_filter.rename) \
            .get()
        post_processing = df.distinct()
        sku_pop_count = post_processing.select(lit("global_popular"). alias( "recommender_id"),
                                               col('sku_id').alias('item_id'),
                                               "pop").distinct()
        sorted_sku_pop = sku_pop_count.sort(desc("pop")). limit(config.properties.item_count)
        df = sorted_sku_pop.groupBy('recommender_id').agg(collect_list(struct("item_id", "pop")).alias("items"))
        df = StrategyUtil.add_common_fields(df,
                                            version=config.properties.version,
                                            tenant=config.properties.tenant_key,
                                            ttl=config.properties.ttl_sec)
        df = df. \
            withColumn("id", concat(lit(config.properties.doc_id), lit('_'), lit(config.properties.version))). \
            withColumn( 'partition_key', lit(config.properties.partition_key)). \
            withColumn(' recommender_id', lit(config.properties.doc_id)). \
            fillna ("")

        return df