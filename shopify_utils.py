import json
import requests

class ShopifyClient:
    def __init__(self, client_id, client_secret):
        self.domain = "xxxxxxx.myshopify.com"
        self.base_url = "https://{}:{}@{}/admin/api/2020-04/".format(client_id, client_secret, self.domain)
        self.session = requests.Session()
        self.session.headers.update({'content-type': 'application/json'})

    def get_locations(self):
        locations = []
        url = self.base_url + "locations.json"
        response = self.session.get(url)

        if response.status_code == 200:
            locations = json.loads(response.text)['locations']

        return locations

    def get_inventorylevels_by_location(self, id):
        try:
            inventory_level_list = []
            page_size = 250 #
            url = self.base_url + "inventory_levels.json?location_ids={}&limit={}".format(id, page_size)
            response = self.session.get(url)
            next_link = None
            if 'Link' in response.headers:
                next_link = response.headers['Link'].replace('<', '').replace('>; rel="next"', '').replace(
                    'https://{}/admin/api/2020-04/'.format(self.domain), self.base_url)

            inventory_levels = json.loads(response.text)['inventory_levels']
            inventory_level_list.extend(inventory_levels)

            while len(inventory_levels) == page_size and next_link is not None:
                response = self.session.get(next_link)
                inventory_levels = json.loads(response.text)['inventory_levels']
                inventory_level_list.extend(inventory_levels)

                if '>; rel="next"' not in response.headers['Link']:
                    break

                next_link = None
                if 'Link' in response.headers:
                    next_link = response.headers['Link'].split(', ')[1].replace('<','').replace('>; rel="next"', '')\
                        .replace('https://{}/admin/api/2020-04/'.format(self.domain), self.base_url)

            return inventory_level_list
        except Exception as e:
            print(e)

    def get_inventorylevels_by_location_ex(self, location_id):
        inventory_level_list = []
        page_size = 250 #
        url = self.base_url + "locations/{}/inventory_levels.json?limit={}".format(location_id, page_size)
        response = self.session.get(url)

        next_link = None
        if 'Link' in response.headers:
            next_link = response.headers['Link'].replace('<', '').replace('>; rel="next"', '')\
                .replace('https://{}/admin/api/2020-04/'.format(self.domain), self.base_url)

        inventory_levels = json.loads(response.text)['inventory_levels']
        inventory_level_list.extend(inventory_levels)

        while len(inventory_levels) == page_size and next_link is not None:
            response = self.session.get(next_link)
            inventory_levels = json.loads(response.text)['inventory_levels']
            inventory_level_list.extend(inventory_levels)

            if '>; rel="next"' not in response.headers['Link']:
                break

            next_link = None
            if 'Link' in response.headers:
                next_link = response.headers['Link'].split(', ')[1].replace('<','').replace('>; rel="next"', '')\
                    .replace('https://{}/admin/api/2020-04/'.format(self.domain), self.base_url)

        return inventory_level_list

    def get_inventorylevel_from_inventory_item_id(self, id):
        levels = []
        url = self.base_url + "inventory_levels.json?inventory_item_ids={}".format(id)
        response = self.session.get(url)

        if response.status_code == 200:
            levels = json.loads(response.text)['inventory_levels']
            print(levels)

        return levels