class Tables(object):

    def __init__(self, node_tables):
        # property
        self.namespace = ''
        self.config = ''
        self.db_type = ''
        self.prefix = ''

        # args
        self.__node_tables = node_tables

        # function
        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node_tables.items():
            if not hasattr(self, k):
                raise TypeError('tables key error:' + k)
            else:
                setattr(self, k, v)
