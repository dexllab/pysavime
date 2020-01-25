class Queue:

    def __init__(self):
        self.__container = []

    def pop(self):
        return self.__container.pop(0)

    def front(self):
        return self.__container[0]

    def push(self, value):
        self.__container.append(value)

    def empty(self):
        return len(self.__container) == 0

    def __repr__(self):
        return self.__container.__repr__()

    def __iter__(self):
        self.ix = 0
        return self

    def __next__(self):
        if self.ix < len(self.__container):
            qi = self.__container[self.ix]
            self.ix += 1
            return qi
        else:
            raise StopIteration

    def __getitem__(self, item):
        return self.__container[item]
