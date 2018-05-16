class Join(object):
    JOIN_KEY = ('type', 'table', 'table_prefix')
    COND_KEY = ('table1', 'table_prefix1', 'field1', 'table2', 'table_prefix2', 'field2', 'comp')

    def __init__(self, node):
        self.node = node
        self.join = self.deal_xml(node)

    def deal_xml(self, node):
        join = {
            'type': 'inner',
            'cond': []
        }
        if not node.hasAttribute('table'):
            raise AttributeError('join must need table')

        for (k, v) in node.attributes.items():
            if k in self.JOIN_KEY:
                join[k] = v
            else:
                raise AttributeError('select.join')

        if node.hasChildNodes():
            for child_node in node.childNodes:
                if child_node.nodeType == child_node.ELEMENT_NODE:
                    if child_node.tagName == 'cond':
                        join['cond'].append(self.deal_cond(child_node))
                    else:
                        raise AttributeError('join has attribute <cond> only')
        else:
            raise TypeError('select.join')

        if len(join['cond']) == 0:
            raise AttributeError('join has not <cond>')

        return join

    def deal_cond(self, node):
        cond = {}
        if node.hasChildNodes():
            # 有嵌套
            if node.hasAttribute('type') and node.getAttribute('type') in ('or', 'and'):
                cond['type'] = node.getAttribute('type')
                cond['child'] = []

                for child_node in node.childNodes:
                    if child_node.nodeType == child_node.ELEMENT_NODE:
                        cond['child'].append(self.deal_cond(child_node))
            else:
                raise TypeError('join.cond type error')
        else:
            # 无嵌套
            if node.hasAttribute('table1') and node.getAttribute('table1'):
                cond['table1'] = node.getAttribute('table1')
            else:
                raise AttributeError('join.cond table1 error')

            if node.hasAttribute('field1') and node.getAttribute('field1'):
                cond['field1'] = node.getAttribute('field1')
            else:
                raise AttributeError('join.cond field1 error')

            if node.hasAttribute('table2') and node.getAttribute('table2'):
                cond['table2'] = node.getAttribute('table2')
            else:
                raise AttributeError('join.cond table2 error')

            if node.hasAttribute('field2') and node.getAttribute('field2'):
                cond['field2'] = node.getAttribute('field2')
            else:
                raise AttributeError('join.cond field2 error')

            if node.hasAttribute('table_prefix1') and node.getAttribute('table_prefix1'):
                cond['table_prefix1'] = node.getAttribute('table_prefix1')

            if node.hasAttribute('table_prefix2') and node.getAttribute('table_prefix2'):
                cond['table_prefix2'] = node.getAttribute('table_prefix2')

            if node.hasAttribute('comp') and node.getAttribute('comp'):
                cond['comp'] = self.replace_spec_string(node.getAttribute('comp'))

        return cond

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
