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

class ESDatasetActivityModel:
    def __init__(self):
        self.activity_type = ''
        self.activity_time = ''
        self.container_code = ''
        self.user = ''
        self.target_name = ''
        self.version = ''
        self.changes = []

    def to_dict(self) -> dict:
        return {
            'activity_type': self.activity_type,
            'activity_time': self.activity_time,
            'container_code': self.container_code,
            'user': self.user,
            'target_name': self.target_name,
            'version': self.version,
            'changes': self.changes
        }
