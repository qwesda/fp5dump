from array import array
import struct
from binascii import hexlify

from .block import Block, TokenType


class BlockChainIter(object):
    """An iterator for the blocks in a block chain.
    Can be initiated with a block id to start the iteration at this block.
    """

    def __init__(self, block_chain, from_block_id=None):
        super(BlockChainIter, self).__init__()

        self.block_chain = block_chain
        self.iter = None
        self.file = self.block_chain.fp5file.file
        self.from_block_id = from_block_id

    def __iter__(self):
        self.iter = self.block_chain.order.__iter__()

        if self.from_block_id is not None:
            block_order_pos = self.block_chain.order.index(self.from_block_id)

            self.iter.__setstate__(block_order_pos)

        return self

    def __next__(self):
        block_id = self.iter.__next__()
        block_pos = self.block_chain.fp5file.block_id_to_block_pos[block_id]

        if block_pos == 0x00000800 and self.block_chain.level == 0:
            raise StopIteration

        return Block(self.file, block_pos, block_id=block_id)


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

    def __iter__(self, from_block_id=None):
        iterator = BlockChainIter(self, from_block_id)

        return iterator.__iter__()

    def __len__(self):
        return self.length

    def tokens(self, search_path=b'', start_block=None):
        """A generator that returns all token belonging for a given path."""

        if type(search_path) is list:
            search_path = b'/'.join(hexlify(part) for part in search_path)
        elif type(search_path) is bytes:
            search_path = search_path.lower()

        if start_block is None:
            start_block = self.fp5file.index.find_first_block_id_for_path(search_path)

        token = None
        last_token = None

        for block in BlockChainIter(self, start_block):
            last_token_param = last_token
            last_token = None

            for token in block.tokens(self.fp5file.file, search_path=search_path, last_token=last_token_param):
                if last_token:
                    yield last_token

                last_token = token

                if token is None:
                    break

            if token is None:
                break

    def find_first_block_id_for_path(self, search_path, start_block=None):
        prev_block_id = None

        if type(search_path) is list:
            search_path = b'/'.join(hexlify(part) for part in search_path)
        elif type(search_path) is bytes:
            search_path = search_path.lower()

        if self.level > 0:
            for index_block in BlockChainIter(self, start_block):
                for token in index_block.index_tokens(self.fp5file.file):

                    if token.field_ref == 0 or token.field_ref is None:
                        token_path_with_field_ref = token.path
                    else:
                        token_path_with_field_ref = b'/'.join([token.path, hexlify(token.field_ref.to_bytes((token.field_ref.bit_length() // 8) + 1, byteorder='big'))])

                    if search_path <= token_path_with_field_ref or token_path_with_field_ref.startswith(search_path) \
                            or token_path_with_field_ref.startswith(b'/') \
                            or ((search_path.startswith(token_path_with_field_ref) or token_path_with_field_ref == b'') and (token.field_ref_bin == b'\xFF\xFE' or token.field_ref_bin == b'\xFF\xFF')):
                        if prev_block_id is None:
                            block_id__daughter_order_pos = self.daughter_block_chain.order.index(
                                int.from_bytes(token.data, byteorder='big')
                            )

                            if block_id__daughter_order_pos > 0:
                                prev_block_id = self.daughter_block_chain.order[block_id__daughter_order_pos - 1]
                            else:
                                prev_block_id = int.from_bytes(token.data, byteorder='big')

                        return self.daughter_block_chain.find_first_block_id_for_path(search_path,
                                                                                      start_block=prev_block_id)

                    prev_block_id = int.from_bytes(token.data, byteorder='big')

            return self.daughter_block_chain.find_first_block_id_for_path(search_path, start_block=prev_block_id)

        else:
            for data_block in BlockChainIter(self, start_block):
                for token in data_block.tokens(self.fp5file.file):
                    if token.path.startswith(search_path):
                        return data_block.id
                    elif search_path < token.path:
                        print("could not find block for path %r" % search_path)
                        return None

        print("could not find block for path %r" % search_path)
        return None

    def order_blocks(self):
        file = self.fp5file.file

        if self.level == self.fp5file.block_chain_levels:
            self.order = array('I', [0x00000000])
            self.fp5file.block_id_to_block_pos[0x00000000] = self.first_block_pos
            self.length = 1
        else:
            if self.level > 0:
                self.order = array('I')
            else:
                self.order = array('I', b'\x00\x00\x00\x00' * (self.fp5file.largest_block_id + 1))

            higher_block_chain__first_block = Block(file, self.parent_block_chain.first_block_pos)

            next_block__prev_id = None

            for token in higher_block_chain__first_block.index_tokens(file):
                if token.type == TokenType.IndexToken:
                    next_block__prev_id = int.from_bytes(token.data, byteorder='big')

                    break

            current_block__prev_id = 0x00000000
            prev_block__next_id = 0x00000000

            while next_block__prev_id is not None:
                block_pos = self.first_block_pos if current_block__prev_id == 0x00000000 else \
                    self.fp5file.block_prev_id_to_block_pos[current_block__prev_id]

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

                if self.fp5file.block_id_to_block_pos[current_block__id] == 0x00000000:
                    self.fp5file.block_id_to_block_pos[current_block__id] = block_pos
                else:
                    print("duplicate block_id to block_pos %r -> %r" % (current_block__id, block_pos))

                if self.level > 0:
                    self.order.append(current_block__id)
                else:
                    self.order[self.length] = current_block__id

                self.length += 1

                current_block__prev_id = current_block__id
                prev_block__next_id = current_block__next_id