import xml.dom.minidom as minidom
from db_py.tables import Tables
from db_py.table import Table
from db_py.field import Field
from db_py.index import Index
from db_py.make import Make
from db_py.update import Update
from db_py.delete import Delete
from db_py.sel import Select
from lxml import etree
from db_py.make_redis import MakeRedis
from db_py.make_file import MakeFile


class DB(object):

    def __init__(self, file_path):
        self.tables = ''
        self.file_path = file_path

    def do(self):
        self.deal_xml()
        self.make_file()

    def deal_xml(self):
        xml_str = ''
        with open(self.file_path, 'r', encoding='utf-8') as fp:
            xml_str += fp.read()

        # 替换特殊字符
        xml_str = xml_str.replace('"<"', '"lt"')
        xml_str = xml_str.replace('"<="', '"lteq"')
        xml_str = xml_str.replace('"<>"', '"ltgt"')

        root = minidom.parseString(xml_str).documentElement
        tables = Tables(root)
        # tables.xml = etree.parse(self.file_path)
        xml_str = bytes(bytearray(xml_str, encoding='utf-8'))
        tables.xml = etree.ElementTree(etree.fromstring(xml_str))

        # table
        table_name_list = []
        for elem in root.getElementsByTagName('table'):
            print('table ======> ' + elem.getAttribute('name'))

            table = Table(elem, tables)
            if table.name in table_name_list:
                raise ValueError('table name repeat:' + table.name)
            else:
                table_name_list.append(table.name)

            if table.db_type == 'mysql':

                for node in elem.childNodes:
                    if node.nodeType != node.ELEMENT_NODE:
                        continue

                    if node.nodeName == 'field':
                        table.add_field(Field(node))

                    elif node.nodeName == 'index':
                        # print('deal index')
                        table.add_index(Index(node))

                    elif node.nodeName == 'primary':
                        # print('deal primary')
                        table.add_primary(Index(node))

                    elif node.nodeName == 'update':
                        # print('deal update')
                        table.add_update(Update(node))

                    elif node.nodeName == 'delete':
                        # print('deal delete')
                        table.add_delete(Delete(node))

                    elif node.nodeName == 'select':
                        # print('deal select')
                        table.add_select(Select(node))

                if table.readonly == 'false':
                    v = Field(None)
                    v.name = 'verid'
                    v.type = 'bigint'
                    table.add_field(v, True)

                    c = Field(None)
                    c.name = 'create_time'
                    c.type = 'datetime'
                    table.add_field(c, True)

                    u = Field(None)
                    u.name = 'update_time'
                    u.type = 'datetime'
                    table.add_field(u, True)

                    d = Field(None)
                    d.name = 'del'
                    d.type = 'tinyint'
                    table.add_field(d, True)

            elif table.db_type == 'redis':
                for node in elem.childNodes:
                    if node.nodeType != node.ELEMENT_NODE:
                        continue

                    if node.nodeName == 'field':
                        table.add_field(Field(node))
                    elif node.nodeName == 'index':
                        table.add_index(Index(node))
                    elif node.nodeName == 'primary':
                        table.add_primary(Index(node))
                    else:
                        raise AttributeError(node.nodeName)

            elif table.db_type == 'file':
                for node in elem.childNodes:
                    if node.nodeType != node.ELEMENT_NODE:
                        continue

                    if node.nodeName == 'field':
                        table.add_field(Field(node))
                    else:
                        raise AttributeError(node.nodeName)

            tables.add_table(table)

        self.tables = tables

    def make_file(self):
        n = 1
        for (tb_name, table) in self.tables.table.items():
            if table.db_type == 'mysql':
                make = Make(n, self.tables)

                # 写sql文件
                if table.readonly == 'false':
                    make.make_add_sql(table)
                    make.make_drop_sql(table)
                    make.make_insert_sql(table)
                    make.make_create_sql(table)

                # 写php文件
                make.make_php_file(table)
                n += 1
            elif table.db_type == 'redis':
                make_redis = MakeRedis(self.tables)
                make_redis.make_php_file(table)
            elif table.db_type == 'file':
                make_file = MakeFile(self.tables)
                make_file.make_php_file(table)


if __name__ == '__main__':
    # db = DB('/Users/tracy/work/project/meiyu/dic/task/db.xml')
    db = DB('/Users/tracy/work/project/meiyu/dic/test/db.xml')
    db.do()
