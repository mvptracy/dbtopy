from dbtopy.where import Where


class Delete(object):

    def __init__(self, node):
        self.name = ''
        self.desc = ''
        self.real = 'false'
        self.field_list = {}  # {'id':{'name':'id';'value':'111'}}
        self.where_list = []  # {where_obj, where_obj}

        self.__node = node

        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise TypeError('delete attribute error:' + k)
            else:
                setattr(self, k, v)

        for node in self.__node.childNodes:
            if node.nodeType != node.ELEMENT_NODE:
                continue

            if node.tagName == 'field':
                field = {}
                if node.hasAttribute('name'):
                    field['name'] = node.getAttribute('name')

                if node.hasAttribute('value'):
                    field['value'] = node.getAttribute('value')

                self.field_list[field['name']] = field

            elif node.tagName == 'where':
                self.where_list.append(Where(node))

