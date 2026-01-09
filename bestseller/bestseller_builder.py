import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from common.data_util.data_writer import Datawriter
from common.db.cosmos.cosmos_util import Cosmostil
from common.util.date_time import DateTimeUtil
if os. path. exists ('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')
from common.data.order_catalog_util import OrderCatalogUtil
from bestseller.bestseller_base_model import BestSellerBaseModel
from bestseller.config.config_bs_reader import ConfigBestSeller, BEST_SELLER_TYPE
from common.data.order.order_reader import OrderReader
from common.data.catalog.catalog_reader import CatalogReader
from common.util.tenant_util import TENANT
from common.util.dataframe_util import DFFilter

@dataclass
class BestSellerBuilder:
    "'""common config reader"""
    spark: SparkSession
    config: ConfigBestSeller
    tenant: TENANT
    model: BestSellerBaseModel
    type: BEST_SELLER_TYPE

def build(self):
    """ builds bestseller
        read order
        read catalog
        join order <->
        catalog apply filter for valid categories
        processing.transform
        processing.postprocessing
        write to output location"""
    df = self.build_data()
    transformed_df = self.model.transform(df, self.config.properties.join_id)
    processed_df = self.model.post_processing(transformed_df, self.config)
    self.write_to(processed_df)

def write_to(self, df):
    run_date = (datetime.now()).strftime('%Y-%m-%d')
    channel = "online"
    output_dir = os.path.join(self.config.properties.output.path, 'ds=' + run_date, 'channel=' + channel)
    Datawriter.write_to(self.spark, df, output_dir, format="json")
    if self.config.properties.cosmos_props is not None:
        cosmos_util = CosmosUtil(spark=self.spark, props=self.config.properties.cosmos_props)
        cosmos.util.write(output_dir, debug=True)
    else:
        print("we do not push to Cosmos for this tenant.")

def build_data(self) -> DataFrame:
    order_df = self.read_order_data()
    catalog_df = self.read_catalog()
    join_df = OrderCatalogUtil.join_order_catalog(self.spark, order_df, catalog_df)
    return join_df

def read_order_data(self):
    print(f"self.config.period {self.config.period}")
    dt = DateTimeUtil.to_str(self.config.order_from_date)
    path = self.config.get_order_path(self.tenant.value)
    from_dt, to_dt = DateTimeUtil.date_from_period(dt, int(self.config.period))
    print (f"reading order data (from_dt) - (to_dt) → {path}")
    df_order = OrderReader(self.spark) \
               .read_from_to(path=path, from_dt=from_dt, to_dt=to_dt, format="parquet")
    df_filter =  DFFilter(df_order)
    filters = self.config.properties.order_filter
    order_df = df_filter.apply_filter(filters.filters).apply_select(filters.select) \
            .apply_rename(filters.rename).get()

    return order_df.distinct()

def read_catalog(self):
    catalog_path = self.config.get_catalog_path(self.tenant.value)
    reader = CatalogReader(self.spark)
    df_catalog = reader. read(path=catalog_path)
    filters = self.config.properties.catalog_filter
    df_catalog = reader.filter_by_category_level(
        df_catalog,
        filters.filter_category,
        filters.filter_category_path)
    df_filter = DFFilter(df_catalog)

    return df_filter.apply_filter(filters.common.filters) \
        .apply_select(filters.common.select) \
        .apply_rename(filters.common.rename) \
        .get().dropDupLicates()