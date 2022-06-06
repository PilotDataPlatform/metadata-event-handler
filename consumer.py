# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json

from elasticsearch import Elasticsearch
from kafka3 import KafkaConsumer

from config import ELASTICSEARCH_SERVICE
from config import KAKFA_SERVICE
from ESItemModel import ESItemModel
from utils import decode_path_from_ltree

kafka_topics = ['metadata.metadata.items', 'metadata.metadata.extended']
es_index = 'metadata-items'

consumer = KafkaConsumer(bootstrap_servers=[KAKFA_SERVICE])
consumer.subscribe(kafka_topics)

pending_items = {}

def write_item_to_es(es_item: ESItemModel, item_id: str):
    print(f'Writing to ElasticSearch ({item_id})')
    es_client = Elasticsearch(ELASTICSEARCH_SERVICE)
    doc = es_item.to_dict()
    es_client.index(index=es_index, body=doc)
    es_client.close()

def parse_items_message(message):
    item_id = message['payload']['id']
    print(f'Consumed items event ({item_id})')
    es_item = ESItemModel()
    if item_id in pending_items:
        es_item = pending_items[item_id]
    es_item.id = item_id
    es_item.parent = message['payload']['parent']
    es_item.parent_path = decode_path_from_ltree(message['payload']['parent_path'])
    es_item.restore_path = decode_path_from_ltree(message['payload']['restore_path'])
    es_item.archived = message['payload']['archived']
    es_item.type = message['payload']['type']
    es_item.zone = message['payload']['zone']
    es_item.name = message['payload']['name']
    es_item.size = message['payload']['size']
    es_item.owner = message['payload']['owner']
    es_item.container_code = message['payload']['container_code']
    es_item.container_type = message['payload']['container_type']
    es_item.created_time = message['payload']['created_time']
    es_item.last_updated_time = message['payload']['last_updated_time']
    es_item.items_set = True
    if es_item.is_complete():
        pending_items.pop(item_id)
        write_item_to_es(es_item, item_id)
    else:
        pending_items[item_id] = es_item

def parse_extended_message(message):
    item_id = message['payload']['item_id']
    print(f'Consumed extended event ({item_id})')
    es_item = ESItemModel()
    if item_id in pending_items:
        es_item = pending_items[item_id]
    es_item.id = item_id
    item_extra = json.loads(message['payload']['extra'])
    es_item.tags = item_extra['tags']
    es_item.system_tags = item_extra['system_tags']
    es_item.attributes = item_extra['attributes']
    es_item.extended_set = True
    if es_item.is_complete():
        pending_items.pop(item_id)
        write_item_to_es(es_item, item_id)
    else:
        pending_items[item_id] = es_item

print('Metadata event handler started')
for event in consumer:
    message = json.loads(event.value)
    if message['schema']['name'] == 'metadata.metadata.items.Value':
        parse_items_message(message)
    elif message['schema']['name'] == 'metadata.metadata.extended.Value':
        parse_extended_message(message)
