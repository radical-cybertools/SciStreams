# the Protocol decorator
def Protocol(name="", output_names=list(), keymap = dict(), accepted_args=list()):
    @simple_func2classdecorator
    def decorator(f):
        class MyClass:
            _accepted_args = accepted_args
            _keymap = keymap
            _output_names = output_names
            _name = name
    
            def __init__(self, **kwargs):
                    self.kwargs = kwargs
        
            def run(self, **kwargs):
                new_kwargs = self.kwargs.copy()
                new_kwargs.update(kwargs)
                return self.run_explicit(_accepted_args=self._accepted_args, _name=self._name, **new_kwargs)
        
            @delayed(pure=True)
            @parse_sciresults(_keymap, _output_names)
            # need **kwargs to allow extra args to be passed
            def run_explicit(*args, **kwargs):
                # only pass accepted args
                new_kwargs = dict()
                for key in kwargs['_accepted_args']:
                    if key in kwargs:
                        new_kwargs[key] = kwargs[key]
                return f(*args, **new_kwargs)
    
        return MyClass
    return decorator

# Adding functions to an existing class
def NewClassMethod(f):
    def decorator(cls):
        cls.__dict__[f.__name__] = f
        return cls
    return decorator
