class BulkInsertBuffer(object):
    def __init__(self, doc_cls, maxsize=1000):
        """
        Arguments:

        * doc_cls - MongoEngine Document class
        * maxsize - Maximum items in buffer. Default is 1000. 
        """
        self._doc_cls = doc_cls
        self._maxsize = maxsize
        self._items = []
        self._count = 0

    def append(self, obj):
        self._items.append(obj)
        self._count += 1
        if len(self._items) >= self._maxsize:
            self.flush()

    def flush(self):
        if len(self._items):
            self._doc_cls.objects.insert(self._items, load_bulk=False)
            self._items = []

    def __len__(self):
        return len(self._items)

    def count(self):
        return self._count
