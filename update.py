from dbtopy.where import Where


class Update(object):

    def __init__(self, node):
        self.name = ''
        self.desc = ''
        self.lock = 'false'
        self.field_list = {}  # {'id':{'name':'id';'value':'111'}}
        self.where_list = []  # {where_obj, where_obj}
        self.suffix = ''

        self.__node = node

        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise AttributeError('update attribute invalid:' + k)
            else:
                setattr(self, k, v)

        if not self.name:
            raise ValueError('update name invalid')

        for node in self.__node.childNodes:
            if node.nodeType != node.ELEMENT_NODE:
                continue

            if node.tagName == 'field':
                field = {}
                if node.hasAttribute('name'):
                    field['name'] = node.getAttribute('name')
                else:
                    raise AttributeError('update.field.name invalid')

                if node.hasAttribute('value'):
                    field['value'] = node.getAttribute('value')

                self.field_list[field['name']] = field

            elif node.tagName == 'where':
                self.where_list.append(Where(node))
