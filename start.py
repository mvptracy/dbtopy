import xml.etree.ElementTree as ET
from dbtopy.tables import Tables
from dbtopy.table import Table
from dbtopy.field import Field
from dbtopy.index import Index
from dbtopy.make import Make


class DB(object):

    def __init__(self, file_path):
        self.file_path = file_path

    def do(self):
        self.deal_xml()

    def deal_xml(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        tables = Tables(root)

        n = 1
        # table
        for node2 in root:
            if node2.tag == 'table':
                print('table:' + node2.attrib['name'])

                table = Table(node2.attrib, tables)
                if table.db_type == 'mysql':
                    for node3 in node2:
                        attr = node3.attrib
                        if node3.tag == 'field':
                            # print('deal field')
                            table.add_field(Field(attr))

                        elif node3.tag == 'index':
                            # print('deal index')
                            table.add_index(Index(attr))

                        elif node3.tag == 'primary':
                            # print('deal primary')
                            table.add_primary(Index(attr))

                        # elif node3.tag == 'update':
                        #     print('deal update')
                        #
                        # elif node3.tag == 'delete':
                        #     print('deal delete')
                        #
                        # elif node3.tag == 'where':
                        #     print('deal where')
                        #
                        # elif node3.tag == 'select':
                        #     print('deal select')

                    # 写文件
                    make = Make(n)
                    make.make_add_sql(table)
                    make.make_drop_sql(table)
                    make.make_insert_sql(table)
                    make.make_create_sql(table)

                    # 清空数据
                elif table.db_type == 'redis':
                    pass
                elif table.db_type == 'file':
                    pass

                n += 1

    def check_required(self, value, type, name):
        value = value.strip()
        if isinstance(value, type) and value:
            return value
        else:
            raise ValueError(name + ' error')

    def check_use_default(self, name, value):

        value = value.strip()
        if not value:
            return self.__getattribute__(name)
        else:
            return value

    def pr_obj(self, obj):
        print("\n".join(['%s:%s' % item for item in obj.__dict__.items()]))


db = DB('db.xml')
db.do()
