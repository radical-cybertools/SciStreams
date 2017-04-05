from dask import delayed

class class1:
    def __init__(self, a):
        self.a = a

    def get(self):
        return self.a

@delayed(pure_default=True)
def get_class_v1(cls, g):
    a = cls(g)
    return a

@delayed(pure=True)
def get_class_v2(cls, g):
    a = cls(g)
    return a

@delayed(pure=False)
def get_class_v3(cls, g):
    a = cls(g)
    return a

def test_class(func, cls,  num, version):

    a = func(cls, num)
    print("class {} instantiation key \t\t{}".format(version, a.key))

    a = func(cls, num)
    print("class {} instantiation key (again) \t{}".format(version, a.key))

    print("class {} member getattr key \t\t{}".format(version, a.get().key))
    print("class {} member getattr key (again) \t{}".format(version, a.get().key))

    print("computed result : {}".format(a.get().compute()))


test_class(get_class_v1, class1, 1, "v1")
test_class(get_class_v2, class1, 2, "v2")
test_class(get_class_v3, class1, 3, "v3")
