from abc import ABCMeta
from functools import singledispatch

class myClass(metaclass=ABCMeta):

    __slots__ = ()

    @classmethod
    def __subclasshook__(cls, C):
        if isinstance(C, dict):
            return True
        else:
            return False
        #if cls is myClass:
            #return False

a = myClass()

@singledispatch
def foo(a):
    print('regular')

@foo.register(myClass)
def _(a):
    print("myClass version")

b = dict()
foo(a)
print(isinstance(a, myClass))
print(isinstance(b, myClass))

