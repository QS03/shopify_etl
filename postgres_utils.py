import os
import psycopg2

from logger_ex import logger


def postgres_connector(handler):

    def connect_postgres(*args, **kwargs):
        con = psycopg2.connect(
            host=os.environ['REDSHIFT_HOST'],
            port=os.environ['REDSHIFT_PORT'],
            database=os.environ['REDSHIFT_DBNAME'],
            user=os.environ['REDSHIFT_USERNAME'],
            password=os.environ['REDSHIFT_PASSWORD']
        )

        ret = handler(con, *args, **kwargs)

        con.close()

        return ret

    return connect_postgres


def insert_object_as_row(con, schema, table, obj):
    cur = con.cursor()

    columns, values = [], []
    for key, value in obj.items():
        columns.append(key)

        if value is None:
            values.append('NULL')
        else:
            if type(obj[key]) == str:
                value = value.replace("'", "''")
                values.append("'{}'".format(value))
            else:
                values.append(str(value))

    query = """
                INSERT INTO "{}"."{}" ({}) VALUES ({});
            """.format(schema, table, ', '.join(columns), ', '.join(values))

    logger.info(query)

    try:
        cur.execute(query)
        con.commit()
    except Exception as e:
        logger.error(e)


@postgres_connector
def get_inventory_item_ids(con):
    cur = con.cursor()
    cur.execute('''SELECT DISTINCT(inventory_item_id) FROM "shopify"."products__variants";''')
    result_set = cur.fetchall()

    inventory_item_ids = []
    for result in result_set:
        inventory_item_ids.append(result[0])

    return inventory_item_ids


@postgres_connector
def save_inventory_levels(con, inventory_levels):

    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS "Shopify"."inventory_levels" (
              id                                    INT IDENTITY(1, 1),
              inventory_item_id                     BIGINT,
              location_id                           BIGINT,
              available                             INT,
              updated_at                            TIMESTAMP,
              pulled_at                             TIMESTAMP,
              admin_graphql_api_id                  VARCHAR(1000)
          );
          ''')

    con.commit()

    key_list = inventory_levels[0].keys()
    values_list = []
    for inventory_level in inventory_levels:
        inventory_level_values = []
        for value in inventory_level.values():
            if type(value) == str:
                inventory_level_values.append("'{}'".format(value.replace("'", "''")))
            elif value == '':
                inventory_level_values.append('NULL')
            elif value is None:
                inventory_level_values.append('NULL')
            else:
                inventory_level_values.append("{}".format(value))

        values_list.append("({})".format(', '.join(inventory_level_values)))

    query = """
            insert into "Shopify"."inventory_levels" ({}) values {};
        """.format(', '.join(key_list), ', '.join(values_list))
    cur.execute(query)
    con.commit()


@postgres_connector
def save_inventory_levels_ex(con, inventory_levels):

    # Drop Table If Exists...
    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS "Shopify"."inventory_levels_ex";''')
    con.commit()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS "Shopify"."inventory_levels_ex" (
              id                                    INT IDENTITY(1, 1),
              inventory_item_id                     BIGINT,
              location_id                           BIGINT,
              available                             INT,
              updated_at                            TIMESTAMP,
              pulled_at                             TIMESTAMP,
              admin_graphql_api_id                  VARCHAR(1000)
          );
          ''')

    con.commit()

    key_list = inventory_levels[0].keys()
    values_list = []
    for inventory_level in inventory_levels:
        inventory_level_values = []
        for value in inventory_level.values():
            if type(value) == str:
                inventory_level_values.append("'{}'".format(value.replace("'", "''")))
            elif value == '':
                inventory_level_values.append('NULL')
            elif value is None:
                inventory_level_values.append('NULL')
            else:
                inventory_level_values.append("{}".format(value))

        values_list.append("({})".format(', '.join(inventory_level_values)))

    query = """
                insert into "Shopify"."inventory_levels_ex" ({}) values {};
            """.format(', '.join(key_list), ', '.join(values_list))
    cur.execute(query)
    con.commit()