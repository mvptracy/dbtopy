class Tables(object):

    def __init__(self, node_tables):
        # property
        self.namespace = ''
        self.config = ''
        self.db_type = ''
        self.prefix = ''
        self.table = {}  # table_name:table_obj
        self.xml = None
        self.readonly = 'false'
        self.logic_del = 'true'

        # args
        self.__node_tables = node_tables

        # function
        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node_tables.attributes.items():
            if not hasattr(self, k):
                raise TypeError('tables attribute error:' + k)
            else:
                setattr(self, k, v)

    def add_table(self, table):
        self.table[table.name] = table
