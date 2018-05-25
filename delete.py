from dbtopy.where import Where


class Delete(object):

    def __init__(self, node):
        self.name = ''
        self.desc = ''
        self.real = 'false'
        self.where_list = []  # {where_obj, where_obj}
        self.suffix = ''
        self.primary_cond = ''

        self.__node = node

        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise AttributeError('delete attribute invalid:' + k)
            else:
                setattr(self, k, v)

        if not self.name:
            raise ValueError('delete name invalid')

        for node in self.__node.childNodes:
            if node.nodeType != node.ELEMENT_NODE:
                continue

            if node.tagName == 'where':
                self.where_list.append(Where(node))

