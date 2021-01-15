import os
import time
import json
from datetime import datetime
import psycopg2

from logger_ex import logger
from postgres_utils import save_inventory_levels, save_inventory_levels_ex
from shopify_utils import ShopifyClient

def main():
    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        logger.warning("Started at: {}".format(current_time))

        shopify_client = ShopifyClient(os.environ['SHOPIFY_CLIENT_ID'], os.environ['SHOPIFY_SECRET_KEY'])

        levels = []
        levels_ex = []
        locations = shopify_client.get_locations()

        for location in locations:
            levels.extend(shopify_client.get_inventorylevels_by_location(location['id']))
            levels_ex.extend(shopify_client.get_inventorylevels_by_location_ex(location['id']))

        levels_with_pulled_time = []
        pulled_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.warning(f"Loading for {datetime.now().strftime('%Y-%m-%d')}")

        for level in levels:
            level['pulled_at'] = pulled_time
            levels_with_pulled_time.append(level)

        levels_ex_with_pulled_time = []
        for level in levels_ex:
            level['pulled_at'] = pulled_time
            levels_ex_with_pulled_time.append(level)

        logger.warning('InventoryLevel Count: {}'.format(len(levels_with_pulled_time)))
        save_inventory_levels(levels_with_pulled_time)

        logger.warning('InventoryLevel_Ex Count: {}'.format(len(levels_ex_with_pulled_time)))
        save_inventory_levels_ex(levels_ex_with_pulled_time)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        logger.warning("Ended at: {}".format(current_time))
    except Exception as e:
        logger.error(e)


def handler(event, context):
    main()


if __name__ == "__main__":
    with open('../config.json') as json_file:
        config = json.load(json_file)

    os.environ['REDSHIFT_HOST'] = config['REDSHIFT_HOST']
    os.environ['REDSHIFT_PORT'] = config['REDSHIFT_PORT']
    os.environ['REDSHIFT_DBNAME'] = config['REDSHIFT_DBNAME']
    os.environ['REDSHIFT_USERNAME'] = config['REDSHIFT_USERNAME']
    os.environ['REDSHIFT_PASSWORD'] = config['REDSHIFT_PASSWORD']
    os.environ['SHOPIFY_CLIENT_ID'] = config['SHOPIFY_CLIENT_ID']
    os.environ['SHOPIFY_SECRET_KEY'] = config['SHOPIFY_SECRET_KEY']

    main()

    del os.environ['REDSHIFT_HOST']
    del os.environ['REDSHIFT_PORT']
    del os.environ['REDSHIFT_DBNAME']
    del os.environ['REDSHIFT_USERNAME']
    del os.environ['REDSHIFT_PASSWORD']
    del os.environ['SHOPIFY_CLIENT_ID']
    del os.environ['SHOPIFY_SECRET_KEY']
