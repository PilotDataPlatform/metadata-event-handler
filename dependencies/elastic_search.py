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
from datetime import datetime
from typing import Optional

from elasticsearch import AsyncElasticsearch
from utils import convert_timestamp


async def insert_doc(index: str, message: dict, client: AsyncElasticsearch, item_id: Optional[str] = None):
    if item_id:
        # check if item already exists in elasticsearch
        search_es = await client.search(index=index, query={"match": {"id": item_id}})
        search_hits = search_es['hits']['total']['value']
        if not search_hits:
            await client.index(index=index, body=message)
        else:
            doc_id = search_es['hits']['hits'][0]['_id']
            item_doc = search_es['hits']['hits'][0]['_source']
            # update doc with message changes
            item_doc.update(message)
            item_doc['last_updated_time'] = convert_timestamp(datetime.utcnow())
            await client.index(index=index, body=item_doc, id=doc_id)
    else:
        await client.index(index=index, body=message)

    await client.close()
