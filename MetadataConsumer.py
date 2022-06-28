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

from elasticsearch import AsyncElasticsearch
from aiokafka import AIOKafkaConsumer

from config import ELASTICSEARCH_SERVICE
from config import KAFKA_TOPICS
from config import KAKFA_SERVICE
from ESItemActivityModel import ESItemActivityModel
from ESDatasetActivityModel import ESDatasetActivityModel
from ESItemModel import ESItemModel
from utils import convert_timestamp
from utils import decode_message
from utils import publish_dlq

es_index = {ESItemModel: 'metadata-items', ESItemActivityModel: 'items-activity-logs',
            ESDatasetActivityModel: 'datasets-activity-logs'}


class MetadataConsumer:
    def __init__(self):
        self.pending_items = {}

    async def write_to_es(self, es_doc: Union[ESItemModel, ESItemActivityModel, ESDatasetActivityModel]):
        print('Writing to elastic search')
        es_client = AsyncElasticsearch(ELASTICSEARCH_SERVICE)
        doc = es_doc.to_dict()
        index = es_index[type(es_doc)]
        await es_client.index(index=index, body=doc)
        await es_client.close()

    async def parse_items_message(self, message):
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
        es_item.created_time = message['created_time']
        es_item.last_updated_time = message['last_updated_time']
        es_item.items_set = True
        if es_item.is_complete():
            self.pending_items.pop(item_id)
            await self.write_to_es(es_item)
        else:
            self.pending_items[item_id] = es_item

    async def parse_extended_message(self, message):
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
            await self.write_to_es(es_item)
        else:
            self.pending_items[item_id] = es_item

    async def parse_item_activity_message(self, message):
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
        await self.write_to_es(es_item)

    async def parse_dataset_activity_message(self, message):
        es_dataset = ESDatasetActivityModel()
        es_dataset.activity_type = message['activity_type']
        es_dataset.activity_time = convert_timestamp(message['activity_time'])
        es_dataset.container_code = message['container_code']
        es_dataset.target_name = message['target_name']
        es_dataset.version = message['version']
        es_dataset.user = message['user']
        es_dataset.changes = message['changes']
        await self.write_to_es(es_dataset)

    async def run(self):
        print('Running consumer')
        self.consumer = AIOKafkaConsumer(bootstrap_servers=[KAKFA_SERVICE])
        self.consumer.subscribe(KAFKA_TOPICS)
        await self.consumer.start()
        try:
            async for event in self.consumer:
                topic = event.topic
                message = decode_message(message=event.value, topic=topic)
                if not message:
                    await publish_dlq(event.value)
                else:
                    if topic == 'metadata.metadata.items':
                        await self.parse_items_message(message)
                    elif topic == 'metadata.metadata.extended':
                        await self.parse_extended_message(message)
                    elif topic == 'metadata.items.activity':
                        await self.parse_item_activity_message(message)
                    elif topic == 'dataset.activity':
                        await self.parse_dataset_activity_message(message)
        finally:
            await self.consumer.stop()
