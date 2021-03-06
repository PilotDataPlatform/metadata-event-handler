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
import base64
import io
import json
import math
from datetime import datetime

from fastavro import schema
from fastavro import schemaless_reader
from fastavro import validate


def decode_label_from_ltree(encoded_string: str) -> str:
    missing_padding = math.ceil(len(encoded_string) / 8) * 8 - len(encoded_string)
    if missing_padding:
        encoded_string += '=' * missing_padding
    utf8_string = base64.b32decode(encoded_string.encode('utf-8')).decode('utf-8')
    return utf8_string


def decode_path_from_ltree(encoded_path: str) -> str:
    if encoded_path:
        labels = encoded_path.split('.')
        path = ''
        for label in labels:
            path += f'{decode_label_from_ltree(label)}.'
        return path[:-1]


def convert_timestamp(timestamp: datetime) -> int:
    converted = int(datetime.timestamp(timestamp) * 1000)
    return converted


def decode_message(message: bytes, topic: str) -> dict:
    try:
        if topic in ['metadata.items.activity', 'dataset.activity']:
            imported_schema = schema.load_schema(f'kafka_schema/{topic}.avsc')
            message_reader = io.BytesIO(message)
            message_decoded = schemaless_reader(message_reader, imported_schema)
            # prevent writers schema changes to be promotable to readers schema
            validater = validate(message_decoded, imported_schema, raise_errors=False)
            if not validater:
                return {}
        else:
            message_reader = json.loads(message)
            message_decoded = message_reader['payload']
            if 'extra' in message_decoded:
                item_extra = json.loads(message_decoded['extra'])
                message_decoded['extra'] = item_extra
                # validate path format. Avro schema cannot validate ltree/path_serializer:
            for key in message_decoded.keys():
                if key in ['parent_path', 'restore_path']:
                    message_decoded[key] = decode_path_from_ltree(message_decoded[key])

    except Exception:
        return {}
    return message_decoded
