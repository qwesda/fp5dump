import struct
from array import array

from collections import OrderedDict


def split_field_and_sub_ref(src):
    if 0x00 <= src[0] <= 0x7F:
        field_ref_len = 1
    elif 0x80 <= src[0] <= 0xBF:
        field_ref_len = 2
    elif 0xC0 <= src[0] <= 0xDF:
        field_ref_len = 3
    elif 0xE0 <= src[0] <= 0xEF:
        field_ref_len = 4
    elif 0xF0 <= src[0] <= 0xF7:
        field_ref_len = 5
    else:
        return None, None

    if field_ref_len < len(src):
        return src[:field_ref_len], src[field_ref_len:]
    else:
        return src[:field_ref_len], None


def decode_field_and_sub_ref(src):
    field_ref = None
    field_ref_len = None

    sub_field_ref = 1
    sub_field_ref_len = None

    src_len = len(src)

    if 0x00 <= src[0] <= 0x7F:
        field_ref_len = 1
    elif 0x80 <= src[0] <= 0xBF:
        field_ref_len = 2
    elif 0xC0 <= src[0] <= 0xDF:
        field_ref_len = 3
    elif 0xE0 <= src[0] <= 0xEF:
        field_ref_len = 4
    elif 0xF0 <= src[0] <= 0xF7:
        field_ref_len = 5

    if field_ref_len:
        if field_ref_len == 1 and 0x00 <= src[0] <= 0x7F:
            field_ref = src[0]
        elif field_ref_len == 2 and 0x80 <= src[0] <= 0xBF:
            field_ref = 0x80 + (src[0] - 0x80) * 0x100 + src[1]
        elif field_ref_len == 3 and 0xC0 <= src[0] <= 0xDF:
            field_ref = 0x4080 + (src[0] - 0xC0) * 0x10000 + src[1] * 0x100 + src[2]
        elif field_ref_len == 4 and 0xE0 <= src[0] <= 0xEF:
            field_ref = 0x204080 + (src[0] - 0xE0) * 0x1000000 + src[1] * 0x10000 + src[2] * 0x100 + src[3]
        elif field_ref_len == 5 and 0xF0 <= src[0] <= 0xF7:
            field_ref = 0x10204080 + (src[0] - 0xF0) * 0x100000000 + src[1] * 0x1000000 + src[2] * 0x10000 + src[3] * 0x100 + src[4]

        if field_ref_len < src_len:
            sub_field_ref_len = src_len - field_ref_len

    if sub_field_ref_len:
        src = src[field_ref_len:]

        if sub_field_ref_len == 1 and 0x00 <= src[0] <= 0x7F:
            sub_field_ref = src[0]
        elif sub_field_ref_len == 2 and 0x80 <= src[0] <= 0xBF:
            sub_field_ref = 0x80 + (src[0] - 0x80) * 0x100 + src[1]
        elif sub_field_ref_len == 3 and 0xC0 <= src[0] <= 0xDF:
            sub_field_ref = 0x4080 + (src[0] - 0xC0) * 0x10000 + src[1] * 0x100 + src[2]
        elif sub_field_ref_len == 4 and 0xE0 <= src[0] <= 0xEF:
            sub_field_ref = 0x204080 + (src[0] - 0xE0) * 0x1000000 + src[1] * 0x10000 + src[2] * 0x100 + src[3]
        elif sub_field_ref_len == 5 and 0xF0 <= src[0] <= 0xF7:
            sub_field_ref = 0x10204080 + (src[0] - 0xF0) * 0x100000000 + src[1] * 0x1000000 + src[2] * 0x10000 + src[3] * 0x100 + src[4]

    return field_ref, sub_field_ref


def decode_vli(src, subtract_64=False):
    src_len = len(src)

    # 0x00 - 0x07F
    if src_len == 1 and 0x00 <= src[0] <= 0x7F:
        return src[0] - (0x40 if subtract_64 else 0x00)
    if src_len == 1 and src[0] >= 0x80:
        return src[0]
    # 0x8000 - 0xBFFF
    elif src_len == 2 and 0x80 <= src[0] <= 0xBF:
        return 0x80 + (src[0] - 0x80) * 0x100 + src[1]
    # 0xC00000 - 0xDFFFFF
    elif src_len == 3 and 0xC0 <= src[0] <= 0xDF:
        return 0x4080 + (src[0] - 0xC0) * 0x10000 + src[1] * 0x100 + src[2]
    # 0xE0000000 - 0xEFFFFFFF
    elif src_len == 4 and 0xE0 <= src[0] <= 0xEF:
        return 0x204080 + (src[0] - 0xE0) * 0x1000000 + src[1] * 0x10000 + src[2] * 0x100 + src[3]
    # 0xF000000000 - 0xF7FFFFFFFF
    elif src_len == 5 and 0xF0 <= src[0] <= 0xF7:
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


class BlockChain(object):
    """Saves the blocks (and their order) belonging to one index/data level."""

    def __init__(self, fp5file, level):
        super(BlockChain, self).__init__()

        self.level = level
        self.fp5file = fp5file
        self.order = None
        self.length = 0

        self.first_block_pos = None

        self.parent_block_chain = None
        self.daughter_block_chain = None

    def node(self, search_path=b''):
        for (ref, data) in self.sub_nodes(search_path, yield_children=False):
            return data

        return None

    def sub_nodes(self, search_path=None, yield_children=True, start_node_path=None, token_ids_to_return=None):
        """A generator that returns all token belonging for a given path."""

        if start_node_path is not None and search_path is not None:
            search_path_data_found = False

            if type(start_node_path) is bytes:
                start_node_path = start_node_path.split(b'/')

            if type(search_path) is bytes:
                search_path = search_path.split(b'/')

            start_block_id = self.fp5file.find_first_block_id_for_path(start_node_path)
        elif search_path is None:
            search_path_data_found = True
            start_block_id = self.order[0]
        else:
            search_path_data_found = False

            if type(search_path) is bytes:
                search_path = search_path.split(b'/')

            start_block_id = self.fp5file.find_first_block_id_for_path(search_path)

        search_path_len = len(search_path)
        relative_level_to_search_path = -search_path_len

        file = self.fp5file.file
        order = self.order
        block_id_to_block_pos = self.fp5file.block_id_to_block_pos

        path = []

        is_first_block = True
        block_chain_end_reached = False

        current_block_id = start_block_id
        current_block_order_pos = order.index(current_block_id)
        current_block_file_pos = block_id_to_block_pos[current_block_id]

        current_node_stack = []

        current_node_dict = OrderedDict()
        current_node_bytes = None

        not_interested_in_current_sub_node = False
        current_sub_token_path = None

        while not block_chain_end_reached:
            if not search_path_data_found:
                self.fp5file.logging.debug(" data block 0x%02X 0x%08X @ 0x%08X" % (0, current_block_id, current_block_file_pos))

            file.seek(current_block_file_pos + 0x0A)
            (current_block_skip_bytes, current_block_length) = struct.unpack_from(">HH", file.read(0x04))

            data_len = current_block_length
            data = file.read(data_len)

            if not is_first_block:
                cursor = current_block_skip_bytes - 1
            else:
                cursor = 0

            while cursor < data_len:
                char_at_cursor = data[cursor]

                # skip fast through data we are not interested in
                if not_interested_in_current_sub_node and (char_at_cursor < 0xC0 or char_at_cursor == 0xFF):
                    if 0x01 <= char_at_cursor <= 0x3F:
                        payload_start = cursor + 2 + char_at_cursor
                        payload_end = payload_start + data[payload_start - 1]
                    elif 0x40 <= char_at_cursor <= 0x7F:
                        payload_start = cursor + 2
                        payload_end = payload_start + data[cursor + 1]
                    elif 0x81 <= char_at_cursor <= 0xBF:
                        payload_start = cursor + 1
                        payload_end = payload_start + (data[cursor] - 0x80)
                    elif char_at_cursor == 0xFF:
                        char_at_cursor = data[cursor + 1]

                        if 0x01 <= char_at_cursor <= 0x04:
                            payload_start = cursor + 4 + char_at_cursor
                            payload_end = payload_start + int.from_bytes(data[cursor + 2 + char_at_cursor:cursor + 4 + char_at_cursor], byteorder='big')
                        elif 0x40 <= char_at_cursor < 0x80:
                            payload_start = cursor + 4
                            payload_end = payload_start + int.from_bytes(data[cursor + 2:cursor + 4], byteorder='big')
                        else:
                            self.fp5file.logging.error("unhandled 0xFF token: %r" % data[cursor:cursor + 20])
                            break
                    elif 0x00 == char_at_cursor:
                        payload_start = cursor + 2
                        payload_end = payload_start + data[cursor + 1]
                    else:
                        self.fp5file.logging.error("unhandled token: %r" % data[cursor:cursor + 20])
                        break

                    cursor = payload_end

                    continue

                # Length Check
                if char_at_cursor == 0x01 and data[cursor + 1] == 0xFF and data[cursor + 2] == 0x05:
                    payload_start = cursor + 3
                    payload_end = payload_start + 5
                    length_check = int.from_bytes(data[payload_start:payload_end], byteorder='big')

                    if current_node_bytes:
                        current_node_bytes = b''.join(current_node_bytes)

                        if len(current_node_bytes) != length_check:
                            self.fp5file.logging.error("length check failed %d != %d\n%s" % (length_check, len(current_node_bytes), current_node_bytes))
                            break
                    elif len(current_node_dict) == 1 and b'\x01' in current_node_dict:
                        if len(current_node_dict[b'\x01']) == length_check:
                            current_node_bytes = current_node_dict[b'\x01']
                            current_node_dict.clear()
                        else:
                            self.fp5file.logging.error("length check failed %d != %d\n%s" % (length_check, len(current_node_dict[b'\x01']), current_node_dict[b'\x01']))
                            break
                    elif len(current_node_dict) == 2 and b'\x01' in current_node_dict:
                        if len(current_node_dict[b'\x01']) != length_check:
                            self.fp5file.logging.error("length check failed %d != %d\n%s" % (length_check, len(current_node_dict[b'\x01']), current_node_dict[b'\x01']))
                            break
                    else:
                        # self.fp5file.logging.error("length check found, but no dict[0x41] or bytes")
                        current_node_bytes = None

                    cursor = payload_end

                    continue

                # FieldRefLong
                if 0x01 <= char_at_cursor <= 0x3F:
                    field_ref_len = char_at_cursor
                    field_ref_bin_combined = data[cursor + 1:cursor + 1 + field_ref_len]
                    field_ref_bin, field_sub_ref_bin = split_field_and_sub_ref(field_ref_bin_combined)

                    payload_start = cursor + 2 + char_at_cursor
                    payload_end = payload_start + data[payload_start - 1]

                    if current_node_bytes is None:
                        if token_ids_to_return is None or field_ref_bin_combined == b'\xfc' or \
                                not (relative_level_to_search_path != 0 and not yield_children) and (relative_level_to_search_path != 1 and yield_children) \
                                or field_ref_bin in token_ids_to_return:
                            current_node_dict[field_ref_bin_combined] = data[payload_start:payload_end]
                        # else:
                        #     print(field_ref_bin, data[payload_start:payload_end])

                    else:
                        check_counter = decode_vli(field_ref_bin_combined)

                        if len(current_node_bytes) != check_counter - 1:
                            self.fp5file.logging.error("wrong partial data counter %d != %d" % (check_counter, len(current_node_bytes)))

                            break

                        current_node_bytes.append(data[payload_start:payload_end])

                    cursor = payload_end

                    continue

                # FieldRefSimple
                elif 0x40 <= char_at_cursor <= 0x7F:
                    payload_start = cursor + 2
                    payload_end = payload_start + data[cursor + 1]

                    if current_node_bytes is None:
                        field_ref_bin = bytes([char_at_cursor & 0xBF])

                        if token_ids_to_return is None or \
                                not (relative_level_to_search_path != 0 and not yield_children) and (relative_level_to_search_path != 1 and yield_children) \
                                or field_ref_bin in token_ids_to_return:
                            current_node_dict[field_ref_bin] = data[payload_start:payload_end]
                        # else:
                        #     print(field_ref_bin, data[payload_start:payload_end])
                    else:
                        check_counter = char_at_cursor - 0x40

                        if len(current_node_bytes) == check_counter - 1:
                            current_node_bytes.append(data[payload_start:payload_end])
                        else:
                            self.fp5file.logging.error("wrong partial data counter %d != %d" % (check_counter, len(current_node_bytes)))
                            break

                    cursor = payload_end
                    continue

                # parse 0x8N
                elif 0x81 <= char_at_cursor <= 0xBF:
                    payload_start = cursor + 1
                    payload_end = payload_start + (data[cursor] - 0x80)

                    current_node_dict[data[payload_start:payload_end]] = None

                    cursor = payload_end
                    continue

                # parse 0xC0
                elif char_at_cursor == 0xC0:
                    if current_block_order_pos + 1 == self.length and cursor + 1 == data_len:
                        return None
                    else:
                        if not yield_children and relative_level_to_search_path == 0 and search_path_data_found:
                            yield (None, current_node_dict)

                            return
                        elif yield_children and relative_level_to_search_path == 0 and search_path_data_found:
                            for (field_ref_bin, paylod) in current_node_dict.items():
                                yield (field_ref_bin, paylod)

                            return
                        elif yield_children and relative_level_to_search_path == 1 and search_path_data_found:
                            if current_node_dict:
                                yield (path[-1], current_node_dict)

                                current_node_dict.clear()

                        field_ref_bin = path.pop()
                        relative_level_to_search_path -= 1

                        if not not_interested_in_current_sub_node:
                            parent_node = current_node_stack.pop()

                            if current_node_dict:
                                parent_node[field_ref_bin] = current_node_dict
                            elif current_node_bytes is not None:
                                parent_node[field_ref_bin] = current_node_bytes

                            current_node_dict = parent_node
                            current_node_bytes = None

                        if (relative_level_to_search_path == 1 and yield_children) or (relative_level_to_search_path == 0 and not yield_children):
                            current_sub_token_path = None
                        elif (relative_level_to_search_path == 2 and yield_children) or (relative_level_to_search_path == 1 and not yield_children):
                            current_sub_token_path = path[-1]

                        if not search_path_data_found:
                            not_interested_in_current_sub_node = True
                        elif current_sub_token_path is not None and token_ids_to_return is not None and current_sub_token_path not in token_ids_to_return:
                            not_interested_in_current_sub_node = True
                        else:
                            not_interested_in_current_sub_node = False

                        cursor += 1

                        continue

                # parse 0xCN
                elif 0xC1 <= char_at_cursor < 0xFE:
                    payload_start = cursor + 1
                    payload_end = payload_start + (char_at_cursor - 0xC0)

                    path.append(data[payload_start:payload_end])

                    relative_level_to_search_path += 1

                    if path[:search_path_len] > search_path:
                        return

                    if not search_path_data_found:
                        if start_node_path is not None:
                            if path[:len(start_node_path)] == start_node_path:
                                search_path_data_found = True

                        elif path[:search_path_len] == search_path:
                            search_path_data_found = True

                    if relative_level_to_search_path == 0:
                        current_sub_token_path = None
                    elif relative_level_to_search_path == 1 and not yield_children:
                        current_sub_token_path = path[-1]
                    elif relative_level_to_search_path == 2 and yield_children:
                        current_sub_token_path = path[-1]

                    if not search_path_data_found:
                        not_interested_in_current_sub_node = True
                    elif current_sub_token_path is not None and token_ids_to_return is not None and current_sub_token_path not in token_ids_to_return:
                        not_interested_in_current_sub_node = True
                    else:
                        not_interested_in_current_sub_node = False

                    if not not_interested_in_current_sub_node:
                        if current_node_bytes is not None:
                            current_node_dict = OrderedDict()
                            current_node_dict[b'\x01'] = current_node_bytes

                        current_node_stack.append(current_node_dict)

                        current_node_dict = OrderedDict()
                        current_node_bytes = None

                    cursor = payload_end

                    continue

                # parse 0xFF
                elif char_at_cursor == 0xFF:
                    if current_node_bytes is None:
                        current_node_bytes = []

                    char_at_cursor = data[cursor + 1]

                    # FieldRefLong + DataLong
                    if 0x01 <= char_at_cursor <= 0x04:
                        field_ref_bin = data[cursor + 2:cursor + 2 + char_at_cursor]
                        check_counter = decode_vli(field_ref_bin)

                        payload_start = cursor + 4 + char_at_cursor
                        payload_end = payload_start + int.from_bytes(data[cursor + 2 + char_at_cursor:cursor + 4 + char_at_cursor], byteorder='big')

                    # FieldRefSimple + DataLong
                    elif 0x40 <= char_at_cursor < 0x80:
                        field_ref_bin = data[cursor + 1:cursor + 2]
                        check_counter = decode_vli(field_ref_bin, True)

                        payload_start = cursor + 4
                        payload_end = payload_start + int.from_bytes(data[cursor + 2:cursor + 4], byteorder='big')
                    else:
                        self.fp5file.logging.error("unhandled 0xFF token: %r" % data[cursor:cursor + 20])
                        break

                    if len(current_node_bytes) != check_counter - 1:
                        self.fp5file.logging.error("wrong partial data counter %d expected %d" % (check_counter, len(current_node_bytes) + 1))

                    current_node_bytes.append(data[payload_start:payload_end])

                    cursor = payload_end

                    continue
                # 0x00
                elif 0x00 == char_at_cursor:
                    payload_start = cursor + 2
                    payload_end = payload_start + data[cursor + 1]

                    cursor = payload_end
                    continue
                else:
                    self.fp5file.logging.error("incomplete parsing block data")
                    break

            if cursor != data_len:
                print("Parsing incomplete: expected: %d got: %d" % (cursor, data_len))

                raise Exception("Parsing incomplete")

            is_first_block = False

            if current_block_order_pos < self.length:
                current_block_order_pos += 1
                current_block_id = order[current_block_order_pos]
                current_block_file_pos = block_id_to_block_pos[current_block_id]
            else:
                block_chain_end_reached = True

    def get_first_block_ref(self):
        self.fp5file.file.seek(self.first_block_pos + 0x0E)

        data = self.fp5file.file.read(6)

        if data.startswith(b'\x00\x04'):
            return int.from_bytes(data[2:], byteorder='big')
        else:
            self.fp5file.logging.error("unexpected block chain start sequence: expected data starting with '00 04' got %s" % data)

        return None

    def order_blocks(self):
        file = self.fp5file.file

        block_id_to_block_pos = self.fp5file.block_id_to_block_pos

        self.order = array('I', b'\x00\x00\x00\x00' * self.length)

        order_pos = 0

        if self.level == self.fp5file.block_chain_levels:
            block_id_to_block_pos[0x00000000] = self.first_block_pos
            self.length = 1
        else:
            next_block__prev_id = self.parent_block_chain.get_first_block_ref()

            current_block__prev_id = 0x00000000
            prev_block__next_id = 0x00000000

            while next_block__prev_id is not None:
                if current_block__prev_id == 0x00000000:
                    block_pos = self.first_block_pos
                else:
                    block_pos = self.fp5file.block_prev_id_to_block_pos[current_block__prev_id]

                file.seek(block_pos + 6)
                current_block__next_id = struct.unpack_from(">I", file.read(4))[0]

                if current_block__next_id != 0x00000000:
                    file.seek(self.fp5file.block_prev_id_to_block_pos[next_block__prev_id] + 2)
                    next_block__prev_id = struct.unpack_from(">I", file.read(4))[0]
                else:
                    next_block__prev_id = None

                if next_block__prev_id is not None:
                    current_block__id = next_block__prev_id
                    next_block__prev_id = current_block__next_id
                else:
                    current_block__id = prev_block__next_id
                    next_block__prev_id = None

                if block_id_to_block_pos[current_block__id] == 0x00000000:
                    block_id_to_block_pos[current_block__id] = block_pos
                else:
                    print("duplicate block_id to block_pos %r -> %r" % (current_block__id, block_pos))

                self.order[order_pos] = current_block__id

                order_pos += 1

                current_block__prev_id = current_block__id
                prev_block__next_id = current_block__next_id
