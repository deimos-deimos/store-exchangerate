from airflow import AirflowException
from datetime import datetime, timedelta
from exchangerate.api_utils import request_date_range
from exchangerate.orm_utils import put_rates_to_stage, get_max_dates


def check_state(db_conf, pairs):
    pairs_to_load_recent = {}
    pairs_to_load_history = pairs.copy()

    for row in get_max_dates(db_conf):
        if row.pair in pairs:
            pairs_to_load_recent[row.pair] = row.max_date
            pairs_to_load_history.remove(row.pair)

    return pairs_to_load_recent, pairs_to_load_history


def split_date_range(start, end, max_days=365):
    days = (end - start).days
    while days > max_days:
        yield start, start + timedelta(days=max_days)
        start = start + timedelta(days=max_days)
        days = (end - start).days
    yield start, end


def group_recent_pairs_by_bases(pairs_to_load_recent):
    pairs_by_bases = {}
    for pair, date in pairs_to_load_recent.items():
        base = pair.split('/')[1]
        pairs_by_bases.setdefault(base, {}).update({pair: date})

    return pairs_by_bases


def group_history_pairs_by_bases(pairs_to_load_history):
    history_pairs_by_bases = {}
    for pair in pairs_to_load_history:
        base = pair.split('/')[1]
        history_pairs_by_bases.setdefault(base, []).append(pair)

    return history_pairs_by_bases


def load_recent_pairs(db_conf, pairs_to_load_recent):
    recent_pairs_by_bases = group_recent_pairs_by_bases(pairs_to_load_recent)
    today_utc = datetime.utcnow().date()
    for base, pairs in recent_pairs_by_bases.items():
        symbols_to_search = [key.split('/')[0] for key in pairs.keys()]
        symbols_str = ','.join(symbols_to_search)

        earliest_date = min(pairs.values())
        for start_date, end_date in split_date_range(earliest_date, today_utc):
            resp = request_date_range(start_date,
                                      end_date,
                                      base,
                                      symbols_str)
            if not resp:
                raise AirflowException('Exchange API has not returned data')
            put_rates_to_stage(db_conf, resp, base, symbols_to_search)


def load_history_pairs(db_conf, pairs_to_load_history):
    history_pairs_by_bases = group_history_pairs_by_bases(pairs_to_load_history)
    today_utc = datetime.utcnow().date()
    for base, pairs in history_pairs_by_bases.items():
        symbols_to_search = [pair.split('/')[0] for pair in pairs]
        iter_start_date = today_utc - timedelta(days=365)
        iter_end_date = today_utc
        while symbols_to_search:
            symbols_str = ','.join(symbols_to_search)
            resp = request_date_range(iter_start_date.strftime("%Y-%m-%d"),
                                      iter_end_date.strftime("%Y-%m-%d"),
                                      base,
                                      symbols_str)
            if not resp:
                raise AirflowException('Exchange API has not returned data')
            put_rates_to_stage(db_conf, resp, base, symbols_to_search)
            symbols_left = resp[iter_start_date.strftime("%Y-%m-%d")].keys()
            symbols_to_search = list(set(symbols_left) & set(symbols_to_search))
            print(symbols_to_search)
            if symbols_to_search:
                iter_end_date = iter_start_date - timedelta(days=1)
                iter_start_date = iter_end_date - timedelta(days=365)
