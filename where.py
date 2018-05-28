class Where(object):
    KEY = ('name', 'type', 'like', 'comp', 'suffix', 'table', 'table_prefix', 'value', 'not_like')

    def __init__(self, node):
        self.node = node

        self.w = self.deal_xml(node)

    def deal_xml(self, node):

        w = {}

        if node.hasChildNodes():
            # 嵌套
            if node.hasAttribute('type') and node.getAttribute('type') in ('or', 'and'):
                w['child'] = []
                w['type'] = node.getAttribute('type')
                for (k, v) in node.attributes.items():
                    if k in self.KEY:
                        if k == 'comp':
                            v = self.replace_spec_string(v)
                        w[k] = v
                    else:
                        raise AttributeError('where attribute invalid:' + k)
                for child_node in node.childNodes:
                    if child_node.nodeType == child_node.ELEMENT_NODE:
                        w['child'].append(self.deal_xml(child_node))
            else:
                raise AttributeError('where type invalid')
        else:
            # 无嵌套
            for (k, v) in node.attributes.items():
                if k in self.KEY:
                    if k == 'comp':
                        v = self.replace_spec_string(v)
                    w[k] = v
                else:
                    raise AttributeError('where attribute invalid:' + k)

        return w

    @staticmethod
    def replace_spec_string(s):
        if s == 'lt':
            return '<'
        elif s == 'lteq':
            return '<='
        elif s == 'ltgt':
            return '<>'
        else:
            return s
