import numpy as np
import random
import uuid
import datetime
import requests
import json
import argparse

def parse_cmd_line():
    """Parse the command line and extract the necessary values."""

    parser = argparse.ArgumentParser(description='Send data to an api gateway for Kinesis stream for analytics. By default, the script '
                                                 'will send events infinitely. To stop the script, press Ctrl+C.')

    
    parser.add_argument('--api-url', required=True, type=str,
                        dest='api_gateway_url', help='api gateway url to post events to')
    parser.add_argument('--invalid-events', action='store_true', dest='invalid_events',
                        help='if provided generates invalid events')

    return parser.parse_args()

def getUUIDs(dataType, count):
    uuids = []
    for i in range(0, count):
        uuids.append(str(uuid.uuid4()))
    return uuids

def getIapData():
    purchase_bundles = [{"name": "Starter Bundle", "price": 4.99},
                        {"name": "Power-Up Bundle", "price": 9.99},
                        {"name": "Collector's Bundle", "price": 19.99},
                        {"name": "Special Event Bundle", "price": 14.99},
                        {"name": "VIP Bundle", "price": 49.99}]

    countries = [
        'UNITED STATES',
        'UK',
        'JAPAN',
        'SINGAPORE',
        'AUSTRALIA',
        'BRAZIL',
        'SOUTH KOREA',
        'GERMANY',
        'CANADA',
        'FRANCE'
    ]

    currencies = {
        'UNITED STATES': 'USD',
        'UK': 'GBP',
        'JAPAN': 'JPY',
        'SINGAPORE': 'SGD',
        'AUSTRALIA': 'AUD',
        'BRAZIL': 'BRL',
        'SOUTH KOREA': 'KRW',
        'GERMANY': 'EUR',
        'CANADA': 'CAD',
        'FRANCE': 'EUR'
    }

    platforms = [
        'nintendo_switch',
        'ps4',
        'xbox_360',
        'iOS',
        'android',
        'pc',
    ]
    country_id = str(np.random.choice(countries, 1, p=[
                     0.3, 0.1, 0.2, 0.05, 0.05, 0.02, 0.15, 0.05, 0.03, 0.05])[0])
    bundle = np.random.choice(purchase_bundles, 1, p=[
        0.2, 0.2, 0.2, 0.2, 0.2])[0]
    iap_transaction = {
        'event_data': {
            'item_version': random.randint(1, 2),
            'country_id': country_id,
            'currency_type': currencies[country_id],
            'bundle_name': bundle['name'],
            'amount': bundle['price'],
            'platform': platforms[random.randint(0, 5)],
            'transaction_id': str(uuid.uuid4())
        }
    }

    return iap_transaction


def generate_event(valid_events):
    event_name = 'iap_transaction'
    event_data = getIapData()
    event = {
        'event_version': '1.0.0',
        'event_id': str(uuid.uuid4()),
        'event_name': event_name,
        'event_timestamp': datetime.datetime.now().isoformat(),
        'app_version': str(np.random.choice(['1.0.0', '1.1.0', '1.2.0'], 1, p=[0.05, 0.80, 0.15])[0])
    }
    if not invalid_events:
        event.update(event_data)
    return event

if __name__ == "__main__":
    args = parse_cmd_line()
    api_gateway_url = args.api_gateway_url
    invalid_events = args.invalid_events
    print('posting events..')
    while True:
        iap_event = generate_event(invalid_events)
        iap_event_json = json.dumps(iap_event)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Post the IAP event to API Gateway
        response = requests.post(
            api_gateway_url, headers=headers, data=iap_event_json)

        if response.status_code != 200:
            print(response)
            print(response.text)
