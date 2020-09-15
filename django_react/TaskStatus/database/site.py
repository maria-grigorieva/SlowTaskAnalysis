# Copy of
# https://gitlab.cern.ch/mgrigori/data-popularity/-/blob/6aa97beab83b740b7a672eadef2c53114fc61675/tasks/datasets/site.py
# (Modified to accept cric https certificate)

import urllib.parse
import requests
import json

# Verify the certificate
import urllib3
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs='certs/atlas-cric-2-cern-ch-chain.pem')

# CRIC API
cric_base_url = 'https://atlas-cric.cern.ch/'
url_site = urllib.parse.urljoin(cric_base_url, 'api/atlas/site/query/?json&state=ANY')
url_queue = urllib.parse.urljoin(cric_base_url, 'api/atlas/pandaqueue/query/?json')
cric_sites = json.loads(http.request('GET', url_site).data.decode('utf-8'))
cric_queues = json.loads(http.request('GET', url_queue).data.decode('utf-8'))

# list of countries
url_countries = 'https://raw.githubusercontent.com/mledoze/countries/master/dist/countries.json'
countries = requests.get(url_countries).json()


def get_site_info(queue_name):
    site_country_latitude = 0.0
    site_country_longitude = 0.0

    queue_info = cric_queues.get(queue_name)

    if queue_info:
        site_info = cric_sites.get(queue_info["rc_site"])
    else:
        site_info = cric_sites.get(queue_name)

    if site_info:
        site_country_name = site_info['country'].lower()
        site_country = next((o for o in countries if o['name']['common'].lower() == site_country_name), None)
        if not site_country:
            site_country = next((o for o in countries if o['cca3'].lower() == site_country_name), None)
        if site_country:
            site_country_latitude, site_country_longitude = site_country['latlng']

        latitude = site_info['rcsite']['latitude']
        longitude = site_info['rcsite']['longitude']
        if latitude == 0.0 and not longitude == 0.0:
            latitude, longitude = site_country_latitude, site_country_longitude

        resource_type = queue_info['resource_type'] if queue_info.get('resource_type') else ''

        return {
            'COMPUTINGSITE': queue_name,
            'SITE_LOCATION': f"{latitude}, {longitude}",
            'SITE_COUNTRY': site_info['rcsite']['country'],
            'SITE_RESOURCE_TYPE': resource_type,
            'SITE_COREPOWER': site_info['corepower'],
            'SITE_CORECOUNT': queue_info['corecount'] if queue_info else "",
            'SITE_CLOUD': site_info['cloud'],
            'SITE_TIER_LEVEL': site_info['tier_level'],
            'SITE_STATE': site_info['state'],
            'SITE_NAME': site_info['name']
        }
    else:
        if queue_info:
            country = queue_info['rc_country']
            site_country = next((o for o in countries if o['name']['common'].lower() == country.lower()), 'UNKNOWN')
            if site_country:
                lat, lon = site_country['latlng']
            else:
                lat, lon = [0, 0]

            return {
                'COMPUTINGSITE': queue_name,
                'SITE_LOCATION': f"{lat},{lon}",
                'SITE_COUNTRY': country,
                'SITE_RESOURCE_TYPE': queue_info['resource_type'] if queue_info.get('resource_type') else '',
                'SITE_COREPOWER': queue_info['corepower'] if queue_info.get('corepower') else '',
                'SITE_CORECOUNT': queue_info['corecount'] if queue_info.get('corecount') else '',
                'SITE_CLOUD': queue_info['cloud'] if queue_info.get('cloud') else '',
                'SITE_TIER_LEVEL': queue_info['tier_level'] if queue_info.get('tier_level') else '',
                'SITE_STATE': queue_info['state'] if queue_info.get('state') else '',
                'SITE_NAME': queue_info['rc_site'] if queue_info.get('rc_site') else ''
            }
        else:
            return {
                'COMPUTINGSITE': queue_name,
                'SITE_LOCATION': '0,0',
                'SITE_COUNTRY': 'UNKNOWN',
                'SITE_RESOURCE_TYPE': '',
                'SITE_COREPOWER': '',
                'SITE_CORECOUNT': '',
                'SITE_CLOUD': '',
                'SITE_TIER_LEVEL': '',
                'SITE_STATE': '',
                'SITE_NAME': queue_name
            }
