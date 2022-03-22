from datetime import datetime
from peewee import PostgresqlDatabase, Model, TextField, DateField, DoubleField, DateTimeField, SQL, fn, Proxy

database_proxy = Proxy()


def get_db_from_conf(db_conf):
    return PostgresqlDatabase(db_conf['db_database'],
                              user=db_conf['db_user'],
                              password=db_conf['db_password'],
                              host=db_conf['db_host'],
                              port=db_conf['db_port']
                              )


class ExchangerateStage(Model):
    pair = TextField()
    price_date = DateField()
    price = DoubleField()
    tech_load_date = DateTimeField(
        constraints=[SQL("DEFAULT now()")])

    class Meta:
        database = database_proxy
        schema = 'stage'
        table_name = 'prices_rate'


class ExchangerateModel(Model):
    pair = TextField()
    price_date = DateField()
    price = DoubleField()
    tech_load_date = DateTimeField(
        constraints=[SQL("DEFAULT now()")])

    class Meta:
        database = database_proxy
        schema = 'model'
        table_name = 'prices_rate'
        indexes = (
            (('pair', 'price_date', 'price'), True),
        )


def put_rates_to_stage(db_conf, rates_resp, base, symbols):
    records_to_add = []
    for date, rates in rates_resp.items():
        for symbol, price in rates.items():
            if symbol in symbols:
                new_record = {
                    'pair': '{}/{}'.format(symbol, base),
                    'price_date': datetime.strptime(date, "%Y-%m-%d"),
                    'price': 1 / price
                }
                records_to_add.append(new_record)
    database_proxy.initialize(get_db_from_conf(db_conf))
    database_proxy.connect(reuse_if_open=True)
    with database_proxy.atomic():
        ExchangerateStage.insert_many(records_to_add).execute()
    database_proxy.close()


def get_max_dates(db_conf):
    database_proxy.initialize(get_db_from_conf(db_conf))
    database_proxy.connect(reuse_if_open=True)
    database_proxy.create_tables([ExchangerateStage, ExchangerateModel])
    ExchangerateStage.truncate_table()

    query = (ExchangerateModel
             .select(ExchangerateModel.pair, fn.Max(ExchangerateModel.price_date).alias('max_date'))
             .group_by(ExchangerateModel.pair))

    max_dates = list(query)
    database_proxy.close()
    return max_dates
