import ison
import sys
import os
from dataclasses import dataclass
from pyspark.sql import SparkSession
from strategies.bestseller.core.category_bestseller_model_ca import CategoryBestSellerMode1CA
if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')
from common.util.tenant_util import TENANT
from strategies.bestseller.bestseller_builder import BestSellerBuilder
from strategies.bestseller.config.config_bs_reader import ConfigBestSeller, BEST_SELLER_TYPE
from strategies.bestseller.core.global_bestseller_model import GlobalBestSellerModel
from strategies.bestseller.core.category_bestseller_model import CategoryBestSellerModel

@dataclass
class BestSellerFactory:
    spark: SparkSession
    config: ConfigBestSeller
    tenant: TENANT
    type: str
def get_builder (self):
    model = None
    if self.type == BEST_SELLER_TYPE. GLOBAL. value:
        model = GlobalBestSellerModel(spark=self.spark, tenant=self.tenant)
    elif self.type == BEST_SELLER_TYPE.CATEGORY.value:
        if self. tenant == TENANT.CA_OD:
            model = CategoryBestSellerModelCA(spark=self.spark, tenant=self.tenant)
        else:
            model = CategoryBestSellerModel(spark=self.spark, tenant=self.tenant)
    else:
        raise ValueError (f"Tenant/Mode Not supported - (self. tenant) / {self. type} ")
    return BestSellerBuilder(self.spark, self.config, self. tenant, model, BEST_SELLER_TYPE(self.type))