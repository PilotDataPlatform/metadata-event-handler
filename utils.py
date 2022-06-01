import base64
import math

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
