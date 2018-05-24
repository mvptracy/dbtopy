class Field(object):
    TYPE = ('int', 'varchar', 'text', 'blob', 'float', 'double', 'timestamp')
    INT_TYPE = ('int', 'tinyint', 'smallint', 'desiumint', 'bigint')
    CHARSET_TYPE = ('utf8', 'utf8mb4', 'gbk', 'gb2312')

    def __init__(self, node):
        # property
        self.name = ''
        self.type = ''
        self.size = ''
        self.auto = 'false'
        self.primary = 'false'
        self.null = 'true'
        self.param = ''
        self.value = ''
        self.array = 'false'
        self.map = 'false'
        self.desc = ''
        self.charset = ''
        self.unsigned = 'false'
        self.bind = 'true'

        # args
        self.__node = node

        # function
        if node:
            self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise AttributeError('field attribute invalid:' + k)
            else:
                setattr(self, k, v.strip())

        if not self.name:
            raise ValueError('field name invalid')

        if not self.type:
            raise ValueError('field type invalid')

        if self.type != 'int':
            self.unsigned = 'false'

        if self.type == 'int':
            self.change_int_type()
            self.charset = ''

        return self

    def change_int_type(self):
        if self.size == '1':
            self.type = 'tinyint'
        elif self.size == '2':
            self.type = 'smallint'
        elif self.size == '3':
            self.type = 'desiumint'
        elif self.size == '8':
            self.type = 'bigint'
        else:
            self.type = 'int'
