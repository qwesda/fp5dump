
class DataField(object):
    """"""

    def __init__(self, id, id_bytes, name):
        super(DataField, self).__init__()

        self.id = id
        self.id_bytes = id_bytes
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
            if self.type == 1:
                return "text"
            elif self.type == 2:
                return "numeric"
            elif self.type == 3:
                return "date"
            elif self.type == 4:
                return "time"
            elif self.type == 5:
                return "bytea"
            elif self.type == 6:
                return "text"
            elif self.type == 7:
                return "text"
            elif self.type == 8:
                return "text"

        elif attribute == "psql_cast":
            return "::" + self.psql_type + "[]" if self.repetitions > 1 else ""

    def __repr__(self):
        return "0x%04X %9s[%2d] %5s '%s'" % (self.id, self.typename, self.repetitions, self.stored, self.label)
