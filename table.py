class Table(object):

    def __init__(self, node_table, tables):
        # property
        self.name = ''
        self.desc = ''
        self.namespace = None
        self.config = None
        self.db_type = None
        self.prefix = None
        self.split = 0
        self.readonly = 'false'
        self.logic_del = 'true'
        self.db = ''
        self.engine = ''
        self.charset = 'utf8'
        self.split_time = ''
        self.primary_key = ''
        self.field_list = {}  # field_name:field_obj
        self.field_name_list = ['verid', 'create_time', 'update_time', 'del']
        self.index_list = {}  # index_name:index_obj
        self.index_name_list = []
        self.update = []  # update_obj
        self.delete = []  # delete_obj
        self.select = []  # select_obj

        # args
        self.__node_table = node_table
        self.__tables = tables

        # function
        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node_table.attributes.items():
            if not hasattr(self, k):
                raise TypeError('table attribute error:' + k)
            else:
                setattr(self, k, v)

        # 设置缺省字段
        if self.namespace is None:
            self.namespace = self.__tables.namespace
        if self.config is None:
            self.config = self.__tables.config
        if self.db_type is None:
            self.db_type = self.__tables.db_type
        if self.prefix is None:
            self.prefix = self.__tables.prefix

    def add_field(self, field, default=False):
        if field.name not in self.field_name_list or default:
            self.field_list[field.name] = field
            self.field_name_list.append(field.name)
            if field.primary == 'true':
                self.primary_key = field.name

    def add_index(self, index):
        if index.name not in self.index_name_list:
            self.index_list[index.name] = index
            self.index_name_list.append(index.name)

    def add_primary(self, index):
        self.primary_key = index.value

    def add_update(self, update):
        self.update.append(update)

    def add_delete(self, delete):
        self.delete.append(delete)

    def add_select(self, select):
        self.select.append(select)
