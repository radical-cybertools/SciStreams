'''
Summary of this wrapper
The idea is to create a delayed task graph from general python objects. So
user can write their own objects, and have class methods that compute them with
little modification. All they would need to do is specify their own hashing
routine : self.tokenize(). This would create a hash that represents the current
state of the object. If the user imagines the object to not be trustable,
randomly change over time, then this hash can return a random string.

The wrapper checks if the function is bound by looking for the __self__ method.
If it exists, it then checks for a tokenize routine. If not present, then it
creates a random string for the hash.

NOTE : this is python 3 only. might need to figure out what the alternative is
for python 2


'''
def delayed_wrapper(f):
    def f_new(*args, **kwargs):
        hshlist = list()
        # check if it's a class
        if hasattr(f, "__self__"):
            # it's a bound method
            self = f.__self__
            # now check if it handles it's own tokenizing
            if hasattr(f.__self__, "tokenize"):
                hshlist.append(self.tokenize())
            else:
                # just hash out random string
                hshlist.append(tokenize())
        for arg in args:
            if hasattr(arg, 'tokenize'):
                hshlist.append(arg.tokenize())
            else:
                hshlist.append(arg)
        for key, val in kwargs.items():
            if hasattr(val, 'tokenize'):
                hshlist.append(val.tokenize())
            else:
                hshlist.append((key,val))
        hsh = tokenize(*hshlist, pure=True)
        return delayed(f, name=hsh, pure=True)(*args, **kwargs)
    return f_new
