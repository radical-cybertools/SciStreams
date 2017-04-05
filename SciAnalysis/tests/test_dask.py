# playing around with dask
from dask import compute, delayed

@delayed(pure=True)
class MyClass:
    def __init__(self):
        pass

    def func(self, a, b):
        return a

class MyNonDelayedClass:
    def __init__(self):
        pass

    def func(self, a, b):
        return a

# some made up parameters
a = 1
b = 2

# keys should not change
getfunc1 = MyClass().func
getfunc2 = MyClass().func
print("getattr : {}".format(getfunc1._key))
print("getattr again : {}".format(getfunc2._key))

call1 = getfunc1(a,b)
call2 = getfunc2(a,b)
print("getattr + func call : {}".format(call1._key))
print("getattr + func call again : {}".format(call2._key))

print("forcing purity")
# make function explicitly pure:
getfunc1 = delayed(MyNonDelayedClass().func, pure=True)
getfunc2 = delayed(MyNonDelayedClass().func, pure=True)

call1 = getfunc1(a,b)
call2 = getfunc2(a,b)
print("getattr + func call : {}".format(call1._key))
print("getattr + func call again : {}".format(call2._key))


print("result 1 : {}".format(call1.compute()))
print("result 2 : {}".format(call2.compute()))
