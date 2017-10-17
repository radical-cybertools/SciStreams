'''
    This code is where most of the StreamDoc processing in the application
    layer should reside. Conversions from other interfaces to StreamDoc are
    found in corresponding interface folders.
'''
from functools import wraps, partial
import time
import sys
from uuid import uuid4
# from ..globals import debugcache

from distributed import Future

# convenience routine to return a hash of the streamdoc
# from dask.delayed import tokenize, delayed, Delayed
from dask.base import normalize_token

# decorator to add timeouts
from .timeout import timeout

import numpy as np

from ..config import default_timeout as DEFAULT_TIMEOUT

from ..globals import client


# this class is used to wrap outputs to inputs
# for ex, if a function returns Arguments(12,34, g=23,h=20)
# will assume the output will serve as input f(12,34, g=23, h=20)
# to some function (unless another streamdoc is merged etc)


# routines that add on to stream doc functionality
def select(sdoc, *mapping):
    return sdoc.select(*mapping)


def pack(*args, **kwargs):
    ''' pack arguments into one set of arguments.'''
    return args


def toargs(arg):
    return Arguments(*arg)


def unpack(args):
    ''' assume input is a tuple, split into arguments.'''
    # print("Arguments : {}".format(args))
    return Arguments(*args)


def todict(kwargs):
    ''' assume input is a dictionary, split into kwargs.'''
    return dict(kwargs=kwargs)


def add_attributes(sdoc, attributes={}):
    # print("adding attributes. previous sdoc : {}".format(sdoc))
    newsdoc = StreamDoc(sdoc)
    newsdoc = newsdoc.add(attributes=attributes)
    return newsdoc


def clear_attributes(sdoc):
    newsdoc = StreamDoc(sdoc)
    newsdoc['attributes'] = dict()
    return newsdoc


def to_attributes(sdoc):
    # move kwargs to attributes
    newsdoc = StreamDoc(sdoc)
    # compute result on cluster
    # could be a Future or not
    try:
        kwargs = sdoc['kwargs'].result()
    except AttributeError:
        kwargs = sdoc['kwargs']
    newsdoc['attributes'].update(kwargs)
    newsdoc['kwargs'] = dict()
    # args = newsdoc['args']

    #for i, arg in enumerate(args):
        #name = "arg_{:04d}".format(i)
        #newsdoc['attributes'][name] = arg

    return newsdoc


def get_attributes(sdoc):
    ''' Return attributes as keyword arguments.'''
    return StreamDoc(kwargs=sdoc['attributes'])


def merge(sdocs):
    ''' merge a zipped tuple of streamdocs.'''
    if len(sdocs) < 2:
        raise ValueError("Error, number of sdocs not 2 or greater")
    return sdocs[0].merge(*(sdocs[1:]))


# TODO :  need to fix this
def squash(sdocs):
    ''' Squash results together.


        For ex, a list of sdocs with a 2D np array
            will lead to one sdoc with a 3D np array
        etc.
    '''
    newsdoc = StreamDoc()
    for sdoc in sdocs:
        newsdoc.add(attributes=sdoc['attributes'])
    N = len(sdocs)
    cnt = 0
    newargs = []
    newkwargs = dict()
    for sdoc in sdocs:
        args, kwargs = sdoc['args'], sdoc['kwargs']
        for i, arg in enumerate(args):
            if cnt == 0:
                if isinstance(arg, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(arg.shape)
                    newargs.append(np.zeros(newshape))
                else:
                    newargs.append([])
            if isinstance(arg, np.ndarray):
                newargs[i][cnt] = arg
            else:
                newargs[i].append[arg]

        for key, val in kwargs.items():
            if cnt == 0:
                if isinstance(val, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(val.shape)
                    newkwargs[key] = np.zeros(newshape)
                else:
                    newkwargs[key] = []
            if isinstance(val, np.ndarray):
                newkwargs[key][cnt] = val
            else:
                newkwargs[key].append[val]

        cnt = cnt + 1

    newsdoc.add(args=newargs, kwargs=newkwargs)

    return newsdoc


def make_descriptor(data):
    desc = dict()
    if isinstance(data, np.ndarray):
        desc['shape'] = data.shape

    return desc


def to_event_stream(sdoc, tolist=False):
    event_stream = _to_event_stream(sdoc)
    if tolist:
        event_stream = list(event_stream)
    return event_stream


def _to_event_stream(sdoc):
    ''' Convert a streamdoc to event stream.

        Gives event_stream as generator

        Generates just one event with all data contained.
    '''
    args = sdoc.args
    if len(args) > 0:
        print("Warning: Args is not zero. Making a new streamdoc with args")
        msg = "(Args should be used as a convenience"
        msg += " but efforts should be made to make them kwargs)"
        print(msg)
        newsdoc = StreamDoc(sdoc)
        newsdoc['args'] = []
        for i, arg in enumerate(args):
            argkey = "_arg{:04d}".format(i)
            newsdoc['kwargs'][argkey] = arg
        sdoc = newsdoc
    start = dict(**sdoc.attributes)

    # issue a new uid
    start_uid = str(uuid4())

    start['uid'] = start_uid
    start['time'] = time.time()

    descriptor = dict()
    desc_uid = str(uuid4())
    descriptor['uid'] = desc_uid
    descriptor['time'] = time.time()
    descriptor['run_start'] = start_uid
    descriptor['data_keys'] = dict()
    descriptor['timestamps'] = dict()
    for key, data in sdoc.kwargs.items():
        # TODO : make data specific, add shape, dtype etc
        desc_data = make_descriptor(data)
        descriptor['data_keys'][key] = desc_data
        descriptor['timestamps'][key] = time.time()

    event = dict()
    event_uid = str(uuid4())
    event['uid'] = event_uid

    event['run_start'] = start_uid
    event['time'] = time.time()
    event['descriptor'] = desc_uid
    event['filled'] = dict()
    event['data'] = dict()
    event['timestamps'] = dict()
    event['seq_num'] = 1

    # I could fill descriptor and event at same time,
    # but I worry about time stamps
    for key, data in sdoc.kwargs.items():
        event['data'][key] = data
        event['timestamps'][key] = time.time()
        event['filled'][key] = True

    # finally the stop document (make them all in order
    # just so time stamps are in sequence)
    stop = dict()
    stop_uid = str(uuid4())
    stop['uid'] = stop_uid
    stop['run_start'] = start_uid
    stop['time'] = time.time()
    stop['exit_status'] = "success"

    yield "start", start
    yield "descriptor", descriptor
    yield "event", event
    yield "stop", stop


class StreamDoc(dict):
    def __init__(self, streamdoc=None, args=[], kwargs={},
                 attributes={}):
        ''' A generalized document meant to be parsed by Streams.

            Components:
                attributes :None the metadata
                outputs : a dictionary of outputs from stream
                kwargs : keyword arguments

                statistics : some statistics of the stream that generated this
                    It can be anything, like run_start, run_stop etc
        '''
        # initialize the dictionary class
        super(StreamDoc, self).__init__(self)

        # NOTE : removed args overall
        # initialize the metadata and kwargs
        self['attributes'] = dict()
        self['kwargs'] = dict()
        self['args'] = list()
        self['provenance'] = dict()
        self['checkpoint'] = dict()

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

    # arguments can be a Future
    # so everything involving it should be submitted to cluster
    def add(self, args=[], kwargs={}, attributes={}, statistics={},
            provenance={}, checkpoint={}):
        ''' add args and kwargs'''
        # Note : will overwrite previous kwarg data without checking
        if isinstance(kwargs, Future) or isinstance(self['kwargs'], Future):
            def update_future(old_dict, update_dict):
                new_dict = dict(old_dict)
                new_dict.update(update_dict)
                return new_dict

            self['kwargs'] = client.submit(update_future, self['kwargs'],
                                           kwargs)
        else:
            self['kwargs'].update(kwargs)
        if isinstance(args, Future):
            def update_args(old_args, update_args):
                new_args = list()
                new_args.extend(old_args)
                new_args.extend(update_args)
                return new_args
            self['args'] = client.submit(update_args, self['args'], args)
        else:
            self['args'].extend(args)

        self['attributes'].update(attributes)
        self['statistics'].update(statistics)
        self['provenance'].update(provenance)
        self['checkpoint'].update(checkpoint)

        return self

    @property
    def kwargs(self):
        return self['kwargs']

    @property
    def args(self):
        return self['args']

    @property
    def attributes(self):
        return self['attributes']

    @property
    def statistics(self):
        return self['statistics']

    def get_return(self, elem=None):
        res = client.submit(self._get_return, elem=elem)
        return res

    def _get_return(self, elem=None):
        ''' get what the function would have normally returned.

            returns raw data
            Parameters
            ----------
            elem : optional
                if an integer: get that nth argumen
                if a string : get that kwarg
        '''
        if isinstance(elem, str):
            res = self['kwargs'][elem]
        elif elem is None:
            # return general expected function output
            if len(self['kwargs']) > 0:
                res = dict(self['kwargs'])
            elif len(self['args']) > 0 and len(self['kwargs']) == 0:
                res = self['args']
                if len(res) == 1:
                    # a function with one arg normally returns this way
                    res = res[0]
            else:
                # if it's more complex, then it wasn't a function output
                res = self
        else:
            raise ValueError("elem not understood : {}".format(elem))

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
        streamdoc = StreamDoc(self)
        for newstreamdoc in newstreamdocs:
            streamdoc.updatedoc(newstreamdoc)
        return streamdoc

    def select(self, *mapping):
        try:
            sdoc = self._select(*mapping)
            return sdoc
        except Exception:
            statistics = dict()
            _cleanexit(self.select, statistics)
            new_sdoc = StreamDoc(attributes=self['attributes'])
            new_sdoc['statistics'] = statistics
            return new_sdoc

    def _select(self, *mapping):
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


def _is_streamdoc(doc):
    if isinstance(doc, dict) and '_StreamDoc' in doc:
        return True
    else:
        return False


def parse_streamdoc(name):
    ''' Decorator to parse StreamDocs from functions

        remote : decide whether or not this computation should be submitted to
        the cluster

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
    def streamdoc_dec(f, remote=True):
        @wraps(f)
        def f_new(x, x2=None, **kwargs_additional):
            # add a time out to f
            # TODO : replace with custom time out per stream
            # NOTE : removed timeout for dask implementation
            # f_timeout = timeout(seconds=DEFAULT_TIMEOUT)(f)
            #f_timeout = f
            # print("Running in {}".format(f.__name__))
            if x2 is None:
                # this is for map
                if _is_streamdoc(x):
                    # extract the args and kwargs
                    args = x.args
                    kwargs = x.kwargs
                    attributes = x.attributes
                else:
                    #args = (x,)
                    kwargs = dict()
                    attributes = dict()
            else:
                # this is for accumulate
                if _is_streamdoc(x) and _is_streamdoc(x2):
                    args = x.get_return(), x2.get_return()
                    kwargs = dict()
                    attributes = x.attributes
                    # attributes of x2 overrides x
                    attributes.update(x2.attributes)
                else:
                    raise ValueError("Two normal arguments not accepted")

            def update_kwargs(old_kwargs, in_kwargs):
                new_kwargs = old_kwargs.copy()
                new_kwargs.update(in_kwargs)
                return new_kwargs

            # kwargs is a Future so we need to be careful
            # print(kwargs)
            # print(kwargs_additional)
            if remote:
                kwargs_future = client.submit(update_kwargs, kwargs,
                                              kwargs_additional)
                # print("Sent to cluster")
            else:
                # we don't want to run on cluster
                # check if they're Futures first and turn them to concrete data
                # if yes
                if isinstance(kwargs, Future):
                    kwargs = kwargs.result()
                if isinstance(kwargs_additional, Future):
                    kwargs_additional = kwargs_additional.result()

                kwargs_future = update_kwargs(kwargs, kwargs_additional)

            # print("args: {}".format(args))
            # print("kwargs: {}".format(kwargs))

            # print("kwargs_future : {}".format(kwargs_future))
            # kwargs.update(kwargs_additional)
            # print("args : {}, kwargs : {}".format(args, kwargs))
            # debugcache.append(dict(args=args, kwargs=kwargs,
            # attributes=attributes, funcname=f.__name__))

            # args and kwargs can also be a Future
            # need to take that into account
            def unwrap(f, args, kwargs):
                # at this stage it's assumed cluster has retrieved kwargs
                return f(*args, **kwargs)

            # two layers here:
            # 1. a Future must be returned so we must return using compute on
            # function
            # 2. The kwargs are not determined until we run computation on
            # cluster so we need to modify function to unwrap kwargs once on
            # cluster. We do this with "unwrap", which must wrap the function
            # before client.submit
            # the args and kwargs themselves may also be Futures

            def future_wrapper(f):
                @wraps(f)
                # assumed that args, kwargs come in this order always
                def f_new(args, kwargs):
                    return client.submit(f, args, kwargs)
                return f_new

            fnew = wraps(f)(partial(unwrap, f))
            # re-define f again...
            if remote:
                fnew = future_wrapper(fnew)
                # print("submitting to cluster")
            else:
                # leave as is
                # print("Not submitting to cluster")
                pass

            statistics = dict()
            t1 = time.time()
            try:
                # now run the function
                # print statements for debugging
                # print(kwargs_future)
                # print(kwargs_future.result())
                # print(kwargs_future)
                result = fnew(args, kwargs_future)
                # print(result)
                # print(result.result())
                # print("args : {}".format(args))
                # print("kwargs : {}".format(kwargs))
                statistics['status'] = "Success"
            except TypeError:
                print("Error, inputs do not match function type")
                import inspect
                sig = inspect.signature(f)
                print("(StreamDoc) Error : Input mismatch on function")
                print("This means there is an issue with "
                      "The stream architecture")
                # print("Got {} arguments".format(len(args)))
                if remote:
                    print("(computed the result on cluster)")
                # could be a Future or not
                try:
                    kwargs_computed = kwargs.result()
                except Exception:
                    kwargs_computed = kwargs
                print("Got kwargs : {}".format(list(kwargs_computed.keys())))
                print("But expected : {}".format(sig))
                print("Returning empty result, see error report below")
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
            attributes['function_list'].append(getattr(f, '__name__',
                                               'unnamed'))
            # print("Running function {}".format(f.__name__))
            # instantiate new stream doc
            streamdoc = StreamDoc(attributes=attributes)
            streamdoc['statistics'] = statistics
            # load in attributes
            # Save outputs to StreamDoc
            # parse arguments to an object with args and kwargs members
            # NOTE : Changed API. Need to ALWAYS assume dict
            #arguments_obj = parse_args(result)

            # print("StreamDoc, parse_streamdoc : parsed args :
            # {}".format(arguments_obj.args))
            # streamdoc.add(args=arguments_obj.args, kwargs=arguments_obj.kwargs)
            def clean_kwargs(res):
                if not isinstance(res, dict):
                    res = dict(_arg0=res)
                return res

            if remote:
                result = client.submit(wraps(f)(clean_kwargs), result)
                print("Computation key : {}".format(result.key))
                print("Computation status : {}".format(result.status))
                print("Computation : {}".format(f.__name__))
            else:
                result = clean_kwargs(result)

            streamdoc.add(kwargs=result)

            return streamdoc

        return f_new

    return streamdoc_dec


parse_streamdoc_map = parse_streamdoc("map")
psdm = parse_streamdoc_map
parse_streamdoc_acc = parse_streamdoc("acc")
psda = parse_streamdoc_acc


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
    errorstr += "func name : {}\n".format(getattr(f, '__name__', 'unnamed'))
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
