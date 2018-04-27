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
        for (k, v) in self.__node.items():
            if not hasattr(self, k):
                raise TypeError('index key error:' + k)
            else:
                setattr(self, k, v)

        return self
