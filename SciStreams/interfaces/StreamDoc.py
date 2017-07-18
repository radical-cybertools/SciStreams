'''
    This code is where most of the StreamDoc processing in the application
    layer should reside. Conversions from other interfaces to StreamDoc are
    found in corresponding interface folders.
'''
from functools import wraps, singledispatch
import time
import sys
from uuid import uuid4
from ..globals import debugcache

# convenience routine to return a hash of the streamdoc
# from dask.delayed import tokenize, delayed, Delayed
from dask.delayed import delayed
from dask.base import normalize_token

from .streams import stream_map, stream_accumulate

# this class is used to wrap outputs to inputs
# for ex, if a function returns Arguments(12,34, g=23,h=20)
# will assume the output will serve as input f(12,34, g=23, h=20)
# to some function (unless another streamdoc is merged etc)


class Arguments:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


# @stream_map.register(Delayed)
# def stream_map_delayed(obj, func, **kwargs):
    # return delayed(func)(obj)

# @stream_accumulate.register(Delayed)
# def stream_accumulate_delayed(obj, func, accumulator, **kwargs):
    # return delayed(func)(accumulator, obj)


# general idea : use single dispatch to act differently on different function
# inputs. This allows one to change behaviour of how output args and kwargs are
# sent
@singledispatch
def parse_args(res):
    return Arguments(res)


@parse_args.register(Arguments)
def parse_args_Arguments(res):
    return Arguments(*res.args, **res.kwargs)


class StreamDoc(dict):
    def __init__(self, streamdoc=None, args=(), kwargs={}, attributes={},
                 wrapper=None):
        ''' A generalized document meant to be parsed by Streams.

            Components:
                attributes :None the metadata
                outputs : a dictionary of outputs from stream
                args : a list of args
                kwargs : a list of kwargs
                statistics : some statistics of the stream that generated this
                    It can be anything, like run_start, run_stop etc
        '''
        self._wrapper = wrapper
        # initialize the dictionary class
        super(StreamDoc, self).__init__(self)

        # initialize the metadata and kwargs
        self['attributes'] = dict()
        self['kwargs'] = dict()
        self['args'] = list()

        # these two pieces are specific to the run
        self['statistics'] = dict()
        self['uid'] = str(uuid4())

        # needed to distinguish that it is a StreamDoc by stream methods
        self['_StreamDoc'] = 'StreamDoc v1.0'

        # update
        if streamdoc is not None:
            self.updatedoc(streamdoc)

        # override with args
        self.add(args=args, kwargs=kwargs, attributes=attributes)

    def updatedoc(self, streamdoc):
        # print("in StreamDoc : {}".format(streamdoc))
        self.add(args=streamdoc['args'], kwargs=streamdoc['kwargs'],
                 attributes=streamdoc['attributes'],
                 statistics=streamdoc['statistics'])
        self._wrapper = streamdoc._wrapper

    def add(self, args=[], kwargs={}, attributes={}, statistics={}):
        ''' add args and kwargs'''
        if not isinstance(args, list) and not isinstance(args, tuple):
            args = (args, )

        self['args'].extend(args)
        # Note : will overwrite previous kwarg data without checking
        self['kwargs'].update(kwargs)
        self['attributes'].update(attributes)
        self['statistics'].update(statistics)

        return self

    # def __stream_map__(self, func, **kwargs):
        # return parse_streamdoc("map")(func)(self, **kwargs)

    # def __stream_reduce__(self, func, accumulator):
        # return parse_streamdoc("reduce")(func)(accumulator, self)

    # def __stream_merge__(self, *others):
        # return self.merge(*others)

    @property
    def args(self):
        return self['args']

    @property
    def kwargs(self):
        return self['kwargs']

    @property
    def attributes(self):
        return self['attributes']

    @property
    def statistics(self):
        return self['statistics']

    def get_return(self, elem=None):
        ''' get what the function would have normally returned.

            returns raw data
            Parameters
            ----------
            elem : optional
                if an integer: get that nth argumen
                if a string : get that kwarg
        '''
        if isinstance(elem, int):
            res = self['args'][elem]
        elif isinstance(elem, str):
            res = self['kwargs'][elem]
        elif elem is None:
            # return general expected function output
            if len(self['args']) == 0 and len(self['kwargs']) > 0:
                res = dict(self['kwargs'])
            elif len(self['args']) > 0 and len(self['kwargs']) == 0:
                res = self['args']
                if len(res) == 1:
                    # a function with one arg normally returns this way
                    res = res[0]
            else:
                # if it's more complex, then it wasn't a function output
                res = self

        return res

    def repr(self):
        mystr = "args : {}\n\n".format(self['args'])
        mystr += "kwargs : {}\n\n".format(self['kwargs'])
        mystr += "attributes : {}\n\n".format(self['attributes'])
        mystr += "statistics : {}\n\n".format(self['statistics'])
        return mystr

    def add_attributes(self, **attrs):
        # print("adding attributes : {}".format(attrs))
        self.add(attributes=attrs)
        return self

    def get_attributes(self):
        return StreamDoc(args=self['attributes'])

    def merge(self, *newstreamdocs):
        ''' Merge another streamdoc into this one.
            The new streamdoc's attributes/kwargs will override this one upon
            collison.
        '''
        # print("in merge : {}".format(newstreamdocs[0]))
        streamdoc = StreamDoc(self)
        for newstreamdoc in newstreamdocs:
            streamdoc.updatedoc(newstreamdoc)
        return streamdoc

    def select(self, *mapping):
        ''' remap args and kwargs
            combinations can be any one of the following:


            Some examples:

            (1,)        : map 1st arg to next available arg
            (1, None)   : map 1st arg to next available arg
            'a',        : map 'a' to 'a'
            'a', None   : map 'a' to next available arg
            'a','b'     : map 'a' to 'b'
            1, 'a'      : map 1st arg to 'a'

            The following is NOT accepted:
            (1,2) : this would map arg 1 to arg 2. Use proper ordering instead
            ('a',1) : this would map 'a' to arg 1. Use proper ordering instead

            Notes
            -----
            These *must* be tuples, and the list a list kwarg elems must be
                strs and arg elems must be ints to accomplish this instead
        '''
        # print("IN STREAMDOC -> SELECT")
        # TODO : take args instead
        # if not isinstance(mapping, list):
        # mapping = [mapping]
        streamdoc = StreamDoc(self)
        streamdoc._wrapper = self._wrapper
        newargs = list()
        newkwargs = dict()
        totargs = dict(args=newargs, kwargs=newkwargs)

        for mapelem in mapping:
            if isinstance(mapelem, str):
                mapelem = mapelem, mapelem
            elif isinstance(mapelem, int):
                mapelem = mapelem, None

            # length 1 for strings, repeat, for int give None
            if len(mapelem) == 1 and isinstance(mapelem[0], str):
                mapelem = mapelem[0], mapelem[0]
            elif len(mapelem) == 1 and isinstance(mapelem[0], int):
                mapelem = mapelem[0], None

            oldkey = mapelem[0]
            newkey = mapelem[1]

            if isinstance(oldkey, int):
                oldparentkey = 'args'
            elif isinstance(oldkey, str):
                oldparentkey = 'kwargs'
            else:
                raise ValueError("old key not understood : {}".format(oldkey))

            if newkey is None:
                newparentkey = 'args'
            elif isinstance(newkey, str):
                newparentkey = 'kwargs'
            elif isinstance(newkey, int):
                errorstr = "Integer tuple pairs not accepted."
                errorstr += " This usually comes from trying a (1,1)"
                errorstr += " or ('foo',1) mapping."
                errorstr += "Please try (1,None) or ('foo', None) instead"
                raise ValueError(errorstr)

            if oldparentkey == 'kwargs' and \
               oldkey not in streamdoc[oldparentkey] \
               or oldparentkey == 'args' and \
               len(streamdoc[oldparentkey]) < oldkey:
                errorstr = "streamdoc.select() : Error {} not ".format(oldkey)
                errorstr += "in the {} of ".format(oldparentkey)
                errorstr += "the current streamdoc.\n"

                errorstr += "Details : Tried to map key {}".format(oldkey)
                errorstr += " from {} ".format(oldparentkey)
                errorstr += " to {}\n.".format(newparentkey)
                errorstr += "This usually occurs from selecting "
                errorstr += "a streamdoc with missing information\n"
                errorstr += "(But could also come from missing data)\n"
                raise KeyError(errorstr)

            if newparentkey == 'args':
                totargs[newparentkey].append(streamdoc[oldparentkey][oldkey])
            else:
                totargs[newparentkey][newkey] = streamdoc[oldparentkey][oldkey]

        streamdoc['args'] = totargs['args']
        streamdoc['kwargs'] = totargs['kwargs']

        return streamdoc


def check_sdoc(sdoc):
    return isinstance(sdoc, StreamDoc)


@stream_map.register(StreamDoc)
def stream_map_streamdoc(func, obj, **kwargs):
    # print("in stream map for StreamDoc, obj : {}".format(obj))
    return parse_streamdoc("map")(func)(obj, **kwargs)


@stream_accumulate.register(StreamDoc)
def stream_accumulate_streamdoc(prevobj, nextobj, func=None, **kwargs):
    # print("accumulating a streamdoc")
    return parse_streamdoc("accumulate")(func)(prevobj, nextobj, **kwargs)
    # def __stream_reduce__(self, func, accumulator):
    # return parse_streamdoc("reduce")(func)(accumulator, self)


def _is_streamdoc(doc):
    if isinstance(doc, dict) and '_StreamDoc' in doc:
        return True
    else:
        return False


def parse_streamdoc(name):
    ''' Decorator to parse StreamDocs from functions

    This is a decorator meant to wrap functions that process streams.
        It must make the following two assumptions:
            functions on streams process either one or two arguments:
                - if processing two arguments, it is assumed that the operation
                is an accumulation: newstate = f(prevstate, newinstance)

        Generally, this wrapper should be used hand in hand with a type.
        For example, here, the type is StreamDoc. If a StreamDoc is detected,
        process the inputs/outputs in a more complicated fashion. Else, leave
        function untouched.

        output:
            if a dict, makes a StreamDoc of args where keys are dict elements
            if a tuple, makes a StreamDoc with only arguments else, makes a
            StreamDoc of just one element
    '''
    def streamdoc_dec(f):
        @wraps(f)
        def f_new(x, x2=None, **kwargs_additional):
            # print("Running in {}".format(f.__name__))
            if x2 is None:
                if _is_streamdoc(x):
                    # extract the args and kwargs
                    args = x.args
                    kwargs = x.kwargs
                    attributes = x.attributes
                else:
                    args = (x,)
                    kwargs = dict()
                    attributes = dict()
            else:
                if _is_streamdoc(x) and _is_streamdoc(x2):
                    args = x.get_return(), x2.get_return()
                    kwargs = dict()
                    attributes = x.attributes
                    # attributes of x2 overrides x
                    attributes.update(x2.attributes)
                else:
                    raise ValueError("Two normal arguments not accepted")

            kwargs.update(kwargs_additional)
            # print("args : {}, kwargs : {}".format(args, kwargs))
            debugcache.append(dict(args=args, kwargs=kwargs,
                                   attributes=attributes, funcname=f.__name__))

            statistics = dict()
            t1 = time.time()
            try:
                # now run the function
                result = f(*args, **kwargs)
                # print("args : {}".format(args))
                # print("kwargs : {}".format(kwargs))
                statistics['status'] = "Success"
            except TypeError:
                print("Error, inputs do not match function type")
                import inspect
                sig = inspect.signature(f)
                ba = sig.bind_partial(*args, **kwargs)
                print("Error : Input mismatch on function")
                print("This means there is an issue with "
                      "The stream architecture")
                print("Got {} arguments".format(len(ba.args)))
                print("Got kwargs : {}".format(list(ba.kwargs.keys())))
                print("But expected : {}".format(sig))
                print("Returning empty result")
                result = {}
                _cleanexit(f, statistics)
            except Exception:
                result = {}
                _cleanexit(f, statistics)

            t2 = time.time()
            statistics['runtime'] = t1 - t2
            statistics['runstart'] = t1

            if 'function_list' not in attributes:
                attributes['function_list'] = list()
            else:
                attributes['function_list'] = \
                    attributes['function_list'].copy()
            # print("updated function list:
            # {}".format(attributes['function_list']))
            attributes['function_list'].append(f.__name__)
            # print("Running function {}".format(f.__name__))
            # instantiate new stream doc
            streamdoc = StreamDoc(attributes=attributes)
            # load in attributes
            # Save outputs to StreamDoc
            arguments_obj = parse_args(result)
            # print("StreamDoc, parse_streamdoc : parsed args :
            # {}".format(arguments_obj.args))
            streamdoc.add(args=arguments_obj.args, kwargs=arguments_obj.kwargs)

            return streamdoc

        return f_new

    return streamdoc_dec


def _cleanexit(f, statistics):
    ''' convenience routine
        to log errors from exception for
        function f into statistics dict

        print an error and return the string as well
    '''
    statistics['status'] = "Failure"
    type, value, tb = sys.exc_info()
    err_frame = tb.tb_frame
    err_lineno = tb.tb_lineno
    err_filename = err_frame.f_code.co_filename
    statistics['error_message'] = value
    errorstr = "####################"
    errorstr += "StreamDoc Error Report"
    errorstr += "##################\n"
    errorstr += "time : {}\n".format(time.ctime(time.time()))
    errorstr += "caught exception {}\n".format(value)
    errorstr += "func name : {}\n".format(f.__name__)
    errorstr += "line number {}\n".format(err_lineno)
    errorstr += "in file {}\n".format(err_filename)
    errorstr += "####################"
    errorstr += "####################"
    errorstr += "####################\n"
    print(errorstr)
    return errorstr


# for delayed objects, to ensure caching
# uses dispatch in dask delayed to define hash function
# for the StreamDoc object
@normalize_token.register(StreamDoc)
def tokenize_sdoc(sdoc):
    return normalize_token((sdoc['args'], sdoc['kwargs']))


def delayed_wrapper(name):
    def decorator(f):
        @delayed(pure=True)
        @parse_streamdoc(name)
        @wraps(f)
        def f_new(*args, **kwargs):
            return f(*args, **kwargs)
        return f_new
    return decorator
