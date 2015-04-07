
class DataField(object):
    """"""

    def __init__(self, field_id, field_id_bin, name):
        super(DataField, self).__init__()

        self.field_id = field_id
        self.field_id_bin = field_id_bin
        self.name = name
        self.label = ""
        self.label_bytes = b""
        self.type = None
        self.order = None
        self.repetitions = 0
        self.options = {}
        self.stored = False
        self.indexed = False

    def __getattr__(self, attribute):
        if attribute == "typename":
            if self.type == 1:
                return "TEXT"
            elif self.type == 2:
                return "NUMBER"
            elif self.type == 3:
                return "DATE"
            elif self.type == 4:
                return "TIME"
            elif self.type == 5:
                return "CONTAINER"
            elif self.type == 6:
                return "CALC"
            elif self.type == 7:
                return "SUMMARY"
            elif self.type == 8:
                return "GLOBAL"
            else:
                return "UNKNOWN"

        elif attribute == "psql_type":
            if self.type == 1:  # "TEXT"
                return "text"
            elif self.type == 2:  # "NUMBER"
                return "numeric"
            elif self.type == 3:  # "DATE"
                return "date"
            elif self.type == 4:  # "TIME"
                return "time"
            elif self.type == 5:  # "CONTAINER"
                return "bytea"
            elif self.type == 6:  # "CALC"
                return "text"
            elif self.type == 7:  # "SUMMARY"
                return "text"
            elif self.type == 8:  # "GLOBAL"
                return "text"

        elif attribute == "pg_oid":
            if self.type == 1:  # "TEXT"
                return 0x19
            elif self.type == 2:  # "NUMBER"
                return 0x06A4
            elif self.type == 3:  # "DATE"
                return 0x043A
            elif self.type == 4:  # "TIME"
                return 0x043B
            elif self.type == 5:  # "CONTAINER"
                return 0x11
            elif self.type == 6:  # "CALC"
                return 0x19
            elif self.type == 7:  # "SUMMARY"
                return 0x19
            elif self.type == 8:  # "GLOBAL"
                return 0x19

        elif attribute == "psql_cast":
            return "::" + self.psql_type + "[]" if self.repetitions > 1 else ""

    def __repr__(self):
        return "0x%04X %9s[%2d] %5s '%s'" % (self.field_id, self.typename, self.repetitions, self.stored, self.label)
