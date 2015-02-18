import struct

from binascii import hexlify
from enum import Enum
from collections import namedtuple


class TokenType(Enum):
    xC0 = 0
    xCN = 1
    x8N = 2
    FieldRefAndDataSimple = 3
    FieldRefAndDataLong = 4
    LengthCheck = 5
    IndexToken = 6
    xFA = 7
    xFC = 8
    xFFFF = 9


Token = namedtuple("Token", ['type', 'path', 'field_ref', 'field_sub_ref', 'field_ref_bin', 'data'])


def decode_vli(src, subtract_64=False):
    # 0x00 - 0x07F
    if len(src) == 1 and 0x00 <= src[0] <= 0x7F:
        return src[0] - (0x40 if subtract_64 else 0x00)
    # 0x8000 - 0xBFFF
    elif len(src) == 2 and 0x80 <= src[0] <= 0xBF:
        return 0x80 + (src[0] - 0x80) * 0x100 + src[1]
    # 0xC00000 - 0xDFFFFF
    elif len(src) == 3 and 0xC0 <= src[0] <= 0xDF:
        return 0x4080 + (src[0] - 0xC0) * 0x10000 + src[1] * 0x100 + src[2]
    # 0xE0000000 - 0xEFFFFFFF
    elif len(src) == 4 and 0xE0 <= src[0] <= 0xEF:
        return 0x204080 + (src[0] - 0xE0) * 0x1000000 + src[1] * 0x10000 + src[2] * 0x100 + src[3]
    # 0xF000000000 - 0xF7FFFFFFFF
    elif len(src) == 5 and 0xF0 <= src[0] <= 0xF7:
        return 0x10204080 + (src[0] - 0xF0) * 0x100000000 + src[1] * 0x1000000 + src[2] * 0x10000 + src[3] * 0x100 + src[4]
    else:
        return None


def encode_vli(src):
    if 0x00 <= src <= 0x7F:
        return int.to_bytes(src, length=1, byteorder='big')
    elif 0x80 <= src <= 0x407F:
        return int.to_bytes(((src - 0x80) + 0x8000), length=2, byteorder='big')
    elif 0x4080 <= src <= 0x20407F:
        return int.to_bytes(((src - 0x4080) + 0xC00000), length=3, byteorder='big')
    elif 0x204080 <= src <= 0x1020407F:
        return int.to_bytes(((src - 0x204080) + 0xE0000000), length=4, byteorder='big')
    elif 0x10204080 <= src <= 0x081020407F:
        return int.to_bytes(((src - 0x10204080) + 0xF000000000), length=5, byteorder='big')
    else:
        return None


class Block(object):
    """a 1024 bytes long chunk of data"""

    def __init__(self, file, offset_in_file, block_id=None):
        super(Block, self).__init__()

        self.offset_in_file = offset_in_file

        self.id = block_id

        file.seek(self.offset_in_file)

        (self.flag1, self.flag2, self.prev_id, self.next_id, self.flag3, self.flag4, self.length) = \
            struct.unpack_from(">BBIIBBH", file.read(14))

    def get_block_bytes_from_file(self, file, include_header):
        if include_header:
            file.seek(self.offset_in_file)
            return file.read(self.length + 14) + (0x400 - self.length - 14) * b'\00'
        else:
            file.seek(self.offset_in_file + 14)
            return file.read(self.length)

    def get_first_token_path(self, file):
        data = self.get_block_bytes_from_file(file, False)
        data_len = len(data)

        cursor = 0
        path = []
        path_as_bytes = b''

        while cursor < len(data):
            offset = 0

            char_at_cursor = data[cursor]

            if char_at_cursor == 0xC0:
                if len(path) > 0:
                    path.pop()
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = 1
            elif 0xC1 <= char_at_cursor <= 0xFE:
                payload_start = cursor + 1
                payload_end = payload_start + (char_at_cursor - 0xC0)

                if payload_end <= data_len:
                    path.append(data[payload_start:payload_end])
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = payload_end - cursor
            else:
                return path_as_bytes

            if offset > 0:
                cursor += offset
            else:
                break

        return path_as_bytes

    def tokens(self, file, search_path=b'', last_token=None):
        if type(search_path) is list:
            search_path = b'/'.join(hexlify(part) for part in search_path)
        elif type(search_path) is bytes:
            search_path = search_path.lower()

        search_path_split_len = len(search_path.split(b'/'))

        data = self.get_block_bytes_from_file(file, False)
        data_len = len(data)

        cursor = 0
        path = []
        path_as_bytes = b''

        while cursor < len(data):
            offset = 0
            token = None

            char_at_cursor = data[cursor]

            # Length Check
            if char_at_cursor == 0x01 and data[cursor + 1] == 0xFF and data[cursor + 2] == 0x05:
                payload_start = cursor + 3
                payload_end = payload_start + 5

                if payload_end <= data_len:
                    token = Token(TokenType.LengthCheck, path_as_bytes, None, 1, None,
                                  int.from_bytes(data[payload_start:payload_end], byteorder='big'))
                    offset = payload_end - cursor

            # FieldRefSimple + DataSimple (first in block chain)
            elif char_at_cursor == 0x00:
                payload_start = cursor + 2 + char_at_cursor
                payload_end = payload_start + data[payload_start - 1]

                if payload_end <= data_len:
                    token = Token(TokenType.FieldRefAndDataSimple, path_as_bytes, 0, 1, b'\x00',
                                  data[payload_start:payload_end])
                    offset = payload_end - cursor

            # FieldRefLong + DataSimple
            elif 0x01 <= char_at_cursor <= 0x3F:
                field_ref_len = char_at_cursor
                field_sub_ref_len = 0
                field_sub_ref = 1

                field_ref_bin = data[cursor + 1:cursor + 1 + field_ref_len]

                if field_ref_bin == b'\xFF\xFF' or field_ref_bin == b'\xFA' or field_ref_bin == b'\xFC':
                    payload_start = cursor + 2 + char_at_cursor
                    payload_end = payload_start + data[payload_start - 1]

                    if field_ref_bin == b'\xFF\xFF':
                        token = Token(TokenType.xFFFF, path_as_bytes, None, None, None, data[payload_start:payload_end])
                    elif field_ref_bin == b'\xFA':
                        token = Token(TokenType.xFA, path_as_bytes, None, None, None, data[payload_start:payload_end])
                    elif field_ref_bin == b'\xFC':
                        token = Token(TokenType.xFC, path_as_bytes, None, None, None, data[payload_start:payload_end])

                    offset = payload_end - cursor
                elif field_ref_bin[0] >= 0xF8:
                    payload_start = cursor + 2 + char_at_cursor
                    payload_end = payload_start + data[payload_start - 1]

                    print(path_as_bytes, field_ref_bin, data[payload_start:payload_end])

                    offset = payload_end - cursor
                else:
                    if 0x00 <= field_ref_bin[0] <= 0x7F:
                        field_ref_len = 1
                    elif 0x80 <= field_ref_bin[0] <= 0xBF:
                        field_ref_len = 2
                    elif 0xC0 <= field_ref_bin[0] <= 0xDF:
                        field_ref_len = 3
                    elif 0xE0 <= field_ref_bin[0] <= 0xEF:
                        field_ref_len = 4
                    elif 0xF0 <= field_ref_bin[0] <= 0xF7:
                        field_ref_len = 5
                    else:
                        raise Exception("Parsing incomplete")

                    field_ref = decode_vli(field_ref_bin[0:field_ref_len])

                    if field_ref_len < char_at_cursor:
                        if 0x00 <= field_ref_bin[field_ref_len] <= 0x7F and char_at_cursor - field_ref_len == 1:
                            field_sub_ref_len = 1
                        elif 0x80 <= field_ref_bin[field_ref_len] <= 0xBF and char_at_cursor - field_ref_len == 2:
                            field_sub_ref_len = 2
                        elif 0xC0 <= field_ref_bin[field_ref_len] <= 0xDF and char_at_cursor - field_ref_len == 3:
                            field_sub_ref_len = 3
                        elif 0xE0 <= field_ref_bin[field_ref_len] <= 0xEF and char_at_cursor - field_ref_len == 4:
                            field_sub_ref_len = 4
                        elif 0xF0 <= field_ref_bin[field_ref_len] <= 0xF7 and char_at_cursor - field_ref_len == 5:
                            field_sub_ref_len = 5

                        field_sub_ref = decode_vli(field_ref_bin[field_ref_len:field_ref_len+field_sub_ref_len])

                    payload_start = cursor + 2 + char_at_cursor
                    payload_end = payload_start + data[payload_start - 1]

                    if payload_end <= data_len:
                        token = Token(TokenType.FieldRefAndDataSimple, path_as_bytes,
                                      field_ref,
                                      field_sub_ref,
                                      field_ref_bin,
                                      data[payload_start:payload_end])

                        offset = payload_end - cursor

            # FieldRefSimple + DataSimple
            elif (0x40 <= char_at_cursor <= 0x7F) or char_at_cursor == 0x00:
                field_ref_bin = data[cursor:cursor + 1]
                field_ref = decode_vli(field_ref_bin, True)

                payload_start = cursor + 2
                payload_end = payload_start + data[cursor + 1]

                if payload_end <= data_len:
                    token = Token(TokenType.FieldRefAndDataSimple, path_as_bytes,
                                  field_ref, 0, field_ref_bin,
                                  data[payload_start:payload_end])
                    offset = payload_end - cursor

            # parse 0x8N
            elif 0x81 <= char_at_cursor <= 0xBF:
                payload_start = cursor + 1
                payload_end = payload_start + (data[cursor] - 0x80)

                if payload_end <= data_len:
                    token = Token(TokenType.x8N, path_as_bytes, None, 0, None, [data[payload_start:payload_end]])
                    offset = payload_end - cursor

            # parse 0xC0
            elif char_at_cursor == 0xC0:
                if search_path_split_len == len(path) - 1:
                    token = Token(TokenType.xC0, path_as_bytes, None, 0, None, None)

                if len(path) > 0:
                    path.pop()
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = 1

            # parse 0xCN
            elif 0xC1 <= char_at_cursor <= 0xFE:
                payload_start = cursor + 1
                payload_end = payload_start + (char_at_cursor - 0xC0)

                if payload_end <= data_len:
                    path.append(data[payload_start:payload_end])
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = payload_end - cursor

            # parse 0xFF
            elif char_at_cursor == 0xFF:
                char_at_cursor = data[cursor + 1]

                # FieldRefLong + DataLong
                if 0x01 <= char_at_cursor <= 0x04:
                    field_ref_bin = bytes(data[cursor + 2:cursor + 2 + char_at_cursor])
                    field_ref = decode_vli(field_ref_bin)

                    payload_start = cursor + 4 + char_at_cursor
                    payload_end = payload_start + int.from_bytes(
                        data[cursor + 2 + char_at_cursor:cursor + 4 + char_at_cursor], byteorder='big')

                    if payload_end <= data_len:
                        token = Token(TokenType.FieldRefAndDataLong, path_as_bytes,
                                      field_ref, 0, field_ref_bin,
                                      data[payload_start:payload_end])
                        offset = payload_end - cursor

                # FieldRefSimple + DataLong
                elif 0x40 <= char_at_cursor <= 0xFE:
                    field_ref_bin = data[cursor + 1:cursor + 2]
                    field_ref = decode_vli(field_ref_bin, True)

                    payload_start = cursor + 4
                    payload_end = payload_start + int.from_bytes(data[cursor + 2:cursor + 4], byteorder='big')

                    if payload_end <= data_len:
                        token = Token(TokenType.FieldRefAndDataLong, path_as_bytes,
                                      field_ref, 1, field_ref_bin,
                                      data[payload_start:payload_end])
                        offset = payload_end - cursor

                else:
                    print("missing parsed DataLong")
                    raise Exception("Parsing incomplete")

            else:
                print("missing parsed")
                raise Exception("Parsing incomplete")

            if offset > 0:
                cursor += offset

                if token is not None and (path_as_bytes.startswith(search_path)):
                    if token.type == TokenType.LengthCheck and last_token is not None \
                            and (last_token.type == TokenType.FieldRefAndDataSimple
                                 or last_token.type == TokenType.FieldRefAndDataLong) \
                            and token.path == last_token.path:
                        if token.data == len(last_token.data):
                            continue
                        else:
                            print("token length check failed", token.data, len(last_token.data))

                    if (token.type == TokenType.FieldRefAndDataLong or TokenType.FieldRefAndDataSimple)\
                            and last_token is not None \
                            and last_token.type == TokenType.FieldRefAndDataLong \
                            and token.path == last_token.path:

                        if token.field_ref == last_token.field_ref + 1:
                            if type(last_token.data) is not bytearray:
                                ba = bytearray(last_token.data)
                            else:
                                ba = last_token.data

                            ba.extend(token.data)

                            last_token = Token(last_token.type, last_token.path, token.field_ref, token.field_sub_ref,
                                               token.field_ref_bin, ba)
                            continue
                        else:
                            print("FieldRefAndDataLong wrong following counter", token.field_ref, last_token.field_ref)
                            print(last_token)
                            print(token)

                    elif token.type == TokenType.x8N and last_token is not None \
                            and last_token.type == TokenType.x8N and token.path == \
                            last_token.path:
                        last_token.data.append(token.data[0])
                    else:
                        if last_token:
                            yield last_token

                        last_token = token

                if not path_as_bytes.startswith(search_path) and search_path < path_as_bytes:
                    if last_token:
                        yield last_token

                    yield None
            else:
                break

        if cursor != data_len:
            print("Parsing incomplete: expected: %d got: %d" % (cursor, data_len))

            raise Exception("Parsing incomplete")

        if last_token:
            yield last_token

    def index_tokens(self, file):
        data = self.get_block_bytes_from_file(file, False)
        data_len = len(data)

        cursor = 0
        path = []
        path_as_bytes = b''

        while cursor < len(data):
            offset = 0
            token = None

            char_at_cursor = data[cursor]

            # FieldRefSimple + DataSimple (first in block chain)
            if char_at_cursor == 0x00:
                payload_start = cursor + 2 + char_at_cursor
                payload_end = payload_start + data[payload_start - 1]

                if payload_end <= data_len:
                    token = Token(TokenType.IndexToken, path_as_bytes, 0, 1, b'\x00',
                                  data[payload_start:payload_end])
                    offset = payload_end - cursor

            # FieldRefLong + DataSimple
            elif 0x01 <= char_at_cursor <= 0x3F:
                field_ref_len = char_at_cursor
                field_sub_ref_len = 0
                field_sub_ref = 1

                field_ref_bin = data[cursor + 1:cursor + 1 + field_ref_len]


                if field_ref_bin == b'\xFF\xFF':
                    field_ref = None
                else:
                    if 0x00 <= field_ref_bin[0] <= 0x7F:
                        field_ref_len = 1
                    elif 0x80 <= field_ref_bin[0] <= 0xBF:
                        field_ref_len = 2
                    elif 0xC0 <= field_ref_bin[0] <= 0xDF:
                        field_ref_len = 3
                    elif 0xE0 <= field_ref_bin[0] <= 0xEF:
                        field_ref_len = 4
                    else:
                        field_ref_len = 1

                    field_ref = decode_vli(field_ref_bin[:field_ref_len])

                    if field_ref_len < char_at_cursor:
                        if 0x00 <= field_ref_bin[field_ref_len] <= 0x7F and char_at_cursor - field_ref_len == 1:
                            field_sub_ref_len = 1
                        elif 0x80 <= field_ref_bin[field_ref_len] <= 0xBF and char_at_cursor - field_ref_len == 2:
                            field_sub_ref_len = 2
                        elif 0xC0 <= field_ref_bin[field_ref_len] <= 0xDF and char_at_cursor - field_ref_len == 3:
                            field_sub_ref_len = 3
                        elif 0xE0 <= field_ref_bin[field_ref_len] <= 0xEF and char_at_cursor - field_ref_len == 4:
                            field_sub_ref_len = 4

                        field_sub_ref = decode_vli(field_ref_bin[field_ref_len:field_ref_len+field_sub_ref_len])

                payload_start = cursor + 2 + char_at_cursor
                payload_end = payload_start + data[payload_start - 1]

                if payload_end <= data_len:
                    token = Token(TokenType.IndexToken, path_as_bytes,
                                  field_ref,
                                  field_sub_ref,
                                  field_ref_bin,
                                  data[payload_start:payload_end])

                    offset = payload_end - cursor

            # FieldRefSimple + DataSimple
            elif (0x40 <= char_at_cursor <= 0x7F) or char_at_cursor == 0x00:
                field_ref_bin = data[cursor:cursor + 1]
                field_ref = decode_vli(field_ref_bin, True)

                payload_start = cursor + 2
                payload_end = payload_start + data[cursor + 1]

                if payload_end <= data_len:
                    token = Token(TokenType.IndexToken, path_as_bytes,
                                  field_ref, 0, field_ref_bin,
                                  data[payload_start:payload_end])
                    offset = payload_end - cursor

            # parse 0xC0
            elif char_at_cursor == 0xC0:
                if len(path) > 0:
                    path.pop()
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = 1

            # parse 0xCN
            elif 0xC1 <= char_at_cursor <= 0xFE:
                payload_start = cursor + 1
                payload_end = payload_start + (char_at_cursor - 0xC0)

                if payload_end <= data_len:
                    path.append(data[payload_start:payload_end])
                    path_as_bytes = b'/'.join(hexlify(part) for part in path)

                offset = payload_end - cursor

            else:
                print("missing parsed")
                raise Exception("Parsing incomplete")

            if offset > 0:
                cursor += offset

                if token is not None:
                    yield token
            else:
                break

        if cursor != data_len:
            print("Parsing incomplete: expected: %d got: %d" % (cursor, data_len))

            raise Exception("Parsing incomplete")


    def __str__(self):
        return "@0x%08X - 0x%02X 0x%02X - 0x%08X < 0x%08X > 0x%08X - 0x%02X 0x%02X " % (
            self.offset_in_file, self.flag1, self.flag2, self.prev_id, self.id if self.id is not None else 0xFFFFFFFF,
            self.next_id, self.flag3, self.flag4)
