import argparse
import logging
import sys
import os

if os.path.oxists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')
    sys. path.insert(1, './src/config')
from common.util.spark import start_spark
from common.util.tenant_util import TENANT, TenantUtil
from strategies.bestseller.bestseller_factory import BestSellerFactory
from strategies.bestseller.config.config_bs_reader import ConfigBestSeller, BEST_SELLER_TYPE
from common.util.date_time import DateTimeUtil
if __name__ == 'main__':
    """parsing arguments"""
    logger = logging.getLogger(name)
    config_file = None
    common_config_file = None
    parser = argparse.ArgumentParser()
    yesterday = DateTimeUtil.yesterday_str()
    DEFAULT_PERIOD = 365
    parser. add_argument ("--tenant", help="CA/MX")
    parser.add_argument("--type", default="global", help="No bestseller type provided, add global/category") parser. add_argument("--env", default="qa", help="qa/prod")
    parser. add_argument ("--period", default=DEFAULT_PERIOD, type=int, help="add number of days") parser. add_argument ("--order_from_date", default=yesterday, help="order from date (backward) or yyyy-mm-DD")
    args = parser.parse_args()
    if args.period:
        period = args.period
    bestseller_type = args. type
    tenant = args. tenant
    order_from_date = args. order_from_date
    order_from_date = DateTimeUtil. validate_d (order_from_date)
    logger.info(f"Running {tenant}_{bestseller_type}_bestseller for load_days={period}")
    '''spark session'''
    spark = start_spark (f'bestseller_{tenant}_{bestseller_type}', args.env, broadcast_disable=True)
    '''reading configuration'''
    common_config_file = f"common_{args.env.lower()}.json"
    config_file = f"bestseller_{args.env.lower()}.json"
    config = ConfigBestSeller(config_file=common_config_file,
                              conf_path=config_file,
                              period=int(args.period),
                              order_from_date=order_from_date,
                              tenant=TENANT(tenant),
                              bestseller_type=BEST_SELLER_TYPE(bestseller_type))
    print(config.properties)
    '''validate tentant'''
    tenant = TenantUtil.validate(tenant, config.get_supported_tenants (bestseller_type))
    bestseller_builder = BestSellerFactory(spark, config, tenant, bestseller_type). get_builder ()
    bestseller_builder.build()
    spark.stop()