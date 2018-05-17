class Index(object):

    def __init__(self, node):
        # property
        self.name = ''
        self.value = ''
        self.unique = 'false'

        # args
        self.__node = node

        # function
        self.deal_xml()

    def deal_xml(self):
        for (k, v) in self.__node.attributes.items():
            if not hasattr(self, k):
                raise AttributeError('index attribute invalid:' + k)
            else:
                setattr(self, k, v)

        if self.__node.nodeName != 'primary' and not self.name:
            raise ValueError('index name invalid')

        return self
