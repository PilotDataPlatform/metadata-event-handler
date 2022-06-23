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

from typing import Union

from elasticsearch import Elasticsearch
from kafka3 import KafkaConsumer

from config import ELASTICSEARCH_SERVICE
from config import KAFKA_TOPICS
from config import KAKFA_SERVICE
from ESItemActivityModel import ESItemActivityModel
from ESItemModel import ESItemModel
from utils import convert_timestamp
from utils import decode_message
from utils import publish_dlq

es_index = {ESItemModel: 'metadata-items', ESItemActivityModel: 'items-activity-logs'}


class MetadataConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers=[KAKFA_SERVICE])
        self.consumer.subscribe(KAFKA_TOPICS)
        self.pending_items = {}

    def write_item_to_es(self, es_item: Union[ESItemModel, ESItemActivityModel], item_id: str):
        print(f'Writing to ElasticSearch ({item_id})')
        es_client = Elasticsearch(ELASTICSEARCH_SERVICE)
        doc = es_item.to_dict()
        index = es_index[type(es_item)]
        es_client.index(index=index, body=doc)
        es_client.close()

    def parse_items_message(self, message):
        item_id = message['id']
        print(f'Consumed items event ({item_id})')
        es_item = ESItemModel()
        if item_id in self.pending_items:
            es_item = self.pending_items[item_id]
        es_item.id = item_id
        es_item.parent = message['parent']
        es_item.parent_path = message['parent_path']
        es_item.restore_path = message['restore_path']
        es_item.archived = message['archived']
        es_item.type = message['type']
        es_item.zone = message['zone']
        es_item.name = message['name']
        es_item.size = message['size']
        es_item.owner = message['owner']
        es_item.container_code = message['container_code']
        es_item.container_type = message['container_type']
        es_item.created_time = convert_timestamp(message['created_time'])
        es_item.last_updated_time = convert_timestamp(message['last_updated_time'])
        es_item.items_set = True
        if es_item.is_complete():
            self.pending_items.pop(item_id)
            self.write_item_to_es(es_item, item_id)
        else:
            self.pending_items[item_id] = es_item

    def parse_extended_message(self, message):
        item_id = message['item_id']
        print(f'Consumed extended event ({item_id})')
        es_item = ESItemModel()
        if item_id in self.pending_items:
            es_item = self.pending_items[item_id]
        es_item.id = item_id
        item_extra = message['extra']
        es_item.tags = item_extra['tags']
        es_item.system_tags = item_extra['system_tags']
        es_item.attributes = item_extra['attributes']
        es_item.extended_set = True
        if es_item.is_complete():
            self.pending_items.pop(item_id)
            self.write_item_to_es(es_item, item_id)
        else:
            self.pending_items[item_id] = es_item

    def parse_item_activity_message(self, message):
        item_id = message['item_id']
        print(f'Consumed activity event ({item_id})')
        es_item = ESItemActivityModel()
        es_item.activity_type = message['activity_type']
        es_item.activity_time = convert_timestamp(message['activity_time'])
        es_item.item_id = message['item_id']
        es_item.item_type = message['item_type']
        es_item.item_name = message['item_name']
        es_item.item_parent_path = message['item_parent_path']
        es_item.container_code = message['container_code']
        es_item.container_type = message['container_type']
        es_item.zone = message['zone']
        es_item.user = message['user']
        es_item.imported_from = message['imported_from']
        es_item.changes = message['changes']
        self.write_item_to_es(es_item, item_id)

    def run(self):
        print('Running consumer')
        for event in self.consumer:
            topic = event.topic
            message = decode_message(message=event.value, topic=topic)
            if not message:
                publish_dlq(event.value)
            else:
                if topic == 'metadata.metadata.items':
                    self.parse_items_message(message)
                elif topic == 'metadata.metadata.extended':
                    self.parse_extended_message(message)
                elif topic == 'metadata.items.activity':
                    self.parse_item_activity_message(message)
