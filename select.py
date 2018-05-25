from dbtopy.where import Where
from dbtopy.join import Join


class Select(object):

    def __init__(self, node):
        self.name = ''
        self.desc = ''
        self.suffix = ''
        self.single = 'false'
        self.page = 'false'
        self.logic_del = 'true'
        self.field_list = []  # [{'name':'id';'value':'111'}]
        self.where_list = []  # [where_obj, where_obj]
        self.join_list = []  # [join_obj, join_obj]

        self.__node = node

        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise TypeError('index key error:' + k)
            else:
                setattr(self, k, v)

            if not self.name:
                raise ValueError('select name invalid')

        for i, node in enumerate(self.__node.childNodes):
            if node.nodeType != node.ELEMENT_NODE:
                continue

            if node.tagName == 'field':
                field = {}
                if node.hasAttribute('name'):
                    field['name'] = node.getAttribute('name')
                else:
                    raise AttributeError('select.field')

                if node.hasAttribute('func') and node.getAttribute('func') in ('sum', 'count', 'max', 'min'):
                    field['func'] = node.getAttribute('func')

                if node.hasAttribute('alias'):
                    field['alias'] = node.getAttribute('alias')

                if node.hasAttribute('table'):
                    field['table'] = node.getAttribute('table')

                if node.hasAttribute('table_prefix'):
                    field['table_prefix'] = node.getAttribute('table_prefix')

                if node.hasAttribute('unique') and node.getAttribute('unique') in ('true', 'false'):
                    field['unique'] = node.getAttribute('unique')

                if node.hasAttribute('origin') and node.getAttribute('origin') in ('true', 'false'):
                    field['origin'] = node.getAttribute('origin')

                self.field_list.append(field)

            elif node.tagName == 'where':
                self.where_list.append(Where(node))

            elif node.tagName == 'join':
                self.join_list.append(Join(node))
