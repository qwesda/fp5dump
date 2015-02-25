import struct

from binascii import hexlify


class Block(object):
    """a 1024 bytes long chunk of data"""

    def __init__(self, file, offset_in_file, block_id=None):
        super(Block, self).__init__()

        self.offset_in_file = offset_in_file

        self.id = block_id

        file.seek(self.offset_in_file)

        (self.deleted_flag, self.index_level, self.prev_id, self.next_id, self.skip_bytes, self.length) = \
            struct.unpack_from(">BBIIHH", file.read(14))

    def get_block_bytes_from_file(self, file, include_header, skip_bytes=False):
        if include_header:
            file.seek(self.offset_in_file)
            return file.read(self.length + 14) + (0x400 - self.length - 14) * b'\00'
        else:
            if skip_bytes:
                file.seek(self.offset_in_file + 14 + self.skip_bytes)
                return file.read(self.length)
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

    def __str__(self):
        return "@0x%08X - 0x%02X 0x%02X - 0x%08X < 0x%08X > 0x%08X - 0x%02X 0x%02X " % (
            self.offset_in_file, self.deleted_flag, self.index_level, self.prev_id, self.id if self.id is not None else 0xFFFFFFFF,
            self.next_id, self.skip_bytes, self.flag4)
