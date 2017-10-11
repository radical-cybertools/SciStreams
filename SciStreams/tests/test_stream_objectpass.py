# test passing an object
from dask import delayed, compute, set_options, get
# for testing the caching
from dask.base import normalize_token
# need this to make sure cache is registered
import SciStreams.globals  # noqa

# make everything pure by default
set_options(delayed_pure=True)


def test_object_hash():
    # test that object hashing is working
    class Foo:
        a = 1
        b = 2

    @normalize_token.register(Foo)
    def tokenize_foo(self):
        return normalize_token((self.a, self.b))

    global_list = list()

    def add(foo):
        global_list.append(1)
        return foo.a + foo.b

    # first, verify the hashes are the same
    myobj = Foo()
    first = delayed(add)(myobj)
    myobj2 = Foo()
    second = delayed(add)(myobj2)
    assert first.key == second.key

    # don't test with streams since it should be the same result
    # this better highlights the problem
    compute(first, get=get)
    compute(second, get=get)
    assert global_list == [1]
