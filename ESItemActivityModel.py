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

class ESItemActivityModel:
    def __init__(self):
        self.activity_type = ''
        self.activity_time = ''
        self.item_id = ''
        self.item_type = ''
        self.item_name = ''
        self.item_parent_path = ''
        self.container_code = ''
        self.container_type = ''
        self.zone = 0
        self.user = ''
        self.imported_from = ''
        self.changes = []

    def to_dict(self) -> dict:
        return {
            'activity_type': self.activity_type,
            'activity_time': self.activity_time,
            'item_id': self.item_id,
            'item_type': self.item_type,
            'item_name': self.item_name,
            'item_parent_path': self.item_parent_path,
            'container_code': self.container_code,
            'container_type': self.container_type,
            'zone': self.zone,
            'user': self.user,
            'imported_from': self.imported_from,
            'changes': self.changes
        }
