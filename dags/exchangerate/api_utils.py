import requests


timeseries_url = 'https://api.exchangerate.host/timeseries'


def request_date_range(start, end, base, symbols):
    params = {
        'start_date': start,
        'end_date': end,
        'symbols': symbols,
        'base': base
    }
    print('resuesting {} with {}'.format(timeseries_url, params))
    response = requests.get(timeseries_url, params=params)
    if response.status_code != 200:
        print('got {} status. reson: {}'.format(response.status_code, response.reason))
        return None
    data = response.json()
    if data['success']:
        return data['rates']
