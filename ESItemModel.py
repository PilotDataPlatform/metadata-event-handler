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

class ESItemModel:
    def __init__(self):
        self.items_set = False
        self.extended_set = False
        self.id = ''
        self.parent = ''
        self.parent_path = ''
        self.restore_path = ''
        self.archived = False
        self.type = ''
        self.zone = 0
        self.name = ''
        self.size = 0
        self.owner = ''
        self.container_code = ''
        self.container_type = ''
        self.created_time = ''
        self.last_updated_time = ''
        self.storage_id = ''
        self.location_uri = ''
        self.version = ''
        self.extended_id = ''
        self.tags = []
        self.system_tags = []
        self.template_name = ''
        self.template_id = ''
        self.attributes = []

    def is_complete(self) -> bool:
        return self.items_set and self.extended_set

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'parent': self.parent,
            'parent_path': self.parent_path,
            'restore_path': self.restore_path,
            'archived': self.archived,
            'type': self.type,
            'zone': self.zone,
            'name': self.name,
            'size': self.size,
            'owner': self.owner,
            'container_code': self.container_code,
            'container_type': self.container_type,
            'created_time': self.created_time,
            'last_updated_time': self.last_updated_time,
            'storage_id': self.storage_id,
            'location_uri': self.location_uri,
            'version': self.version,
            'extended_id': self.extended_id,
            'tags': self.tags,
            'system_tags': self.system_tags,
            'template_name': self.template_name,
            'template_id': self.template_id,
            'attributes': self.attributes
        }
