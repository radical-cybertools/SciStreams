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
# from .timeout import timeout

import numpy as np

# from ..config import default_timeout as DEFAULT_TIMEOUT

from ..globals import client


# TODO : Make sure each element is Future aware

# this class is used to wrap outputs to inputs
# for ex, if a function returns Arguments(12,34, g=23,h=20)
# will assume the output will serve as input f(12,34, g=23, h=20)
# to some function (unless another streamdoc is merged etc)
'''
    This is the crux of the analysis control. Everything is emitted as a
    StreamDoc. This is also made friendly for distributed environments. It is
    done by checking whether incoming members are dask.Futures or not.
    Currently, this only supports dask for distributed computing but in
    principle could support other API's, so long as the correct instance of
    Future and client (with members 'submit' and 'compute') are supplied.
'''


# routines that add on to stream doc functionality
def select(sdoc, *mapping):
    return sdoc.select(*mapping)


def pack(*args, **kwargs):
    ''' pack arguments into one set of arguments.'''
    return args


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

    # for i, arg in enumerate(args):
    # name = "arg_{:04d}".format(i)
    # newsdoc['attributes'][name] = arg

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


def to_event_stream(sdoc, tolist=False, remote=True):
    ''' Convert stream documents to an event stream.
    '''
    event_stream = _to_event_stream(sdoc)
    if tolist:
        event_stream = list(event_stream)
    return event_stream


def init_start(start_uid):
    ''' Initialize a start document.
        Can be done remotely so it is it's own function.

        Need uid supplied externally. Separate Futures from uid.
    '''
    doc = dict()

    doc['uid'] = start_uid
    doc['time'] = time.time()
    return doc


def update_start(start, attrs):
    start.update(attrs)
    return start


def init_descriptor(desc_uid, start_uid):
    ''' Initialize the descriptor.

        Need the start_uid and desc_uid
    '''
    descriptor = dict()
    descriptor['uid'] = desc_uid
    descriptor['time'] = time.time()
    descriptor['run_start'] = start_uid
    descriptor['data_keys'] = dict()
    descriptor['timestamps'] = dict()
    return descriptor


def make_descriptor(data):
    ''' Make a descriptor for data (to be used in a descriptor
        This returns a dict describing the data field, not the total data.
    '''
    desc = dict()
    if isinstance(data, np.ndarray):
        desc['shape'] = data.shape

    return desc


def update_descriptor(descriptor, kwargs):
    ''' Update a descriptor according to incoming data.

        kwargs : a dictionary of the incoming data
    '''
    for key, data in kwargs.items():
        # TODO : make data specific, add shape, dtype etc
        desc_data = make_descriptor(data)
        descriptor['data_keys'][key] = desc_data
        descriptor['timestamps'][key] = time.time()
    return descriptor


def init_event(event_uid, start_uid, descriptor_uid):
    ''' Initialize an event

        Need the start_uid and descriptor_uid
    '''
    event = dict()
    event['uid'] = event_uid
    event['run_start'] = start_uid
    event['time'] = time.time()
    event['descriptor'] = descriptor_uid
    event['filled'] = dict()
    event['data'] = dict()
    event['timestamps'] = dict()
    event['seq_num'] = 1

    return event


def update_event(event, kwargs):
    ''' Update an event according to incoming data.

        kwargs : the incoming data
    '''
    # I could fill descriptor and event at same time,
    # but I worry about time stamps
    for key, data in kwargs.items():
        event['data'][key] = data
        event['timestamps'][key] = time.time()
        event['filled'][key] = True
    return event


def init_stop(stop_uid, start_uid):
    ''' Initialize a stop document.

        start : the incoming start document
            (need it to grab the uid)
    '''
    # finally the stop document (make them all in order
    # just so time stamps are in sequence)
    stop = dict()
    stop['uid'] = stop_uid
    stop['run_start'] = start_uid
    stop['time'] = time.time()
    stop['exit_status'] = "success"
    return stop


def _to_event_stream(sdoc):
    ''' Convert a streamdoc to event stream.

        Gives event_stream as generator

        Generates just one event with all data contained.

        NOTE : Does not work with args (only considers kwargs)
            #(will just print a warning and try to add them)
            It will just ignore them now. Could add in future if needed,
            not too difficult.
    '''
    # args = sdoc.args
    # if len(args) > 0:
    #   print("Warning: Args is not zero. Making a new streamdoc with args")
    #   msg = "(Args should be used as a convenience"
    #   msg += " but efforts should be made to make them kwargs)"
    #   print(msg)
    #   newsdoc = StreamDoc(sdoc)
    #   newsdoc['args'] = []
    #   for i, arg in enumerate(args):
    #       argkey = "_arg{:04d}".format(i)
    #       newsdoc['kwargs'][argkey] = arg
    #   sdoc = newsdoc
    attributes = sdoc.attributes
    kwargs = sdoc.kwargs
    sdoc_type = sdoc["_StreamDoc_Type"]
    # it's remote if either is a Future
    # NOTE: could have non remote attributes and remote kwargs, and thus local
    # start. But let's just make all events remote if they are
    isremote = isinstance(kwargs, Future) or isinstance(attributes, Future)\
        or isinstance(sdoc_type, Future)

    # create the uids in advance
    start_uid = str(uuid4())
    descriptor_uid = str(uuid4())
    event_uid = str(uuid4())
    stop_uid = str(uuid4())

    if isremote:
        start = client.submit(init_start, start_uid)
        start = client.submit(update_start, start, attributes)
    else:
        start = init_start(start_uid)
        start = update_start(start, attributes)

    if isremote:
        descriptor = client.submit(init_descriptor, descriptor_uid, start_uid)
        descriptor = client.submit(update_descriptor, descriptor, kwargs)
    else:
        descriptor = init_descriptor(descriptor_uid, start_uid)
        descriptor = update_descriptor(descriptor, kwargs)

    if isremote:
        event = client.submit(init_event, event_uid, start_uid, descriptor_uid)
        event = client.submit(update_event, event, kwargs)
    else:
        event = init_event(event_uid, start_uid, descriptor_uid)
        event = update_event(event, kwargs)

    if isremote:
        stop = client.submit(init_stop, stop_uid, start_uid)
    else:
        stop = init_stop(stop_uid, start_uid)

    # for symmetry, put None for parent
    yield "start", (None, start_uid, start)
    # it's a tree structure so we need parent_uid, and self_uid
    yield "descriptor", (start_uid, descriptor_uid, descriptor)
    yield "event", (descriptor_uid, event_uid, event)
    yield "stop", (start_uid, stop_uid, stop)


class StreamDoc(dict):
    def __init__(self, streamdoc=None, args=[], kwargs={},
                 attributes={}, sdoc_type='full'):
        ''' A generalized document meant to be parsed by Streams.

            sdoc_type : {'full', 'empty', 'error'}
                the type of streamdoc
                empty prevents functions from being run downstream
                error as well

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
        self['statistics'] = dict(cumulative_time=0.)
        self['uid'] = str(uuid4())

        # needed to distinguish that it is a StreamDoc by stream methods
        self['_StreamDoc'] = 'StreamDoc v1.0'
        # to propagate empty values
        # TODO : maybe add 'partial'? (i.e. values need to be filled in)
        self['_StreamDoc_Type'] = sdoc_type

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
        def update_future_dict(old_dict, update_dict):
            new_dict = dict(old_dict)
            new_dict.update(update_dict)
            return new_dict
        # Note : will overwrite previous kwarg data without checking
        # this looks overly complicated, but just checks to see if results are
        # local or on cluster (Future)
        # if anything is a Future, then the operation is sent to cluster
        # Basically, if any computation is already remote, keep it remote
        # if it was all local, then keep local (the latter blocks, so be
        # careful)
        if isinstance(kwargs, Future) or isinstance(self['kwargs'], Future):

            self['kwargs'] = client.submit(update_future_dict, self['kwargs'],
                                           kwargs)
        else:
            self['kwargs'].update(kwargs)

        if isinstance(attributes, Future) or \
                isinstance(self['attributes'], Future):
            self['attributes'] = client.submit(update_future_dict,
                                               self['attributes'],
                                               attributes)
        else:
            self['attributes'].update(attributes)

        if isinstance(args, Future) or isinstance(self['args'], Future):
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
        res = client.submit(_get_return, self.args, self.kwargs, elem=elem)
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
            args, kwargs = self['args'], self['kwargs']
#            if not isinstance(args, Future):
#                print("len args : {}".format(len(args)))
#            else:
#                print("len args : {}".format(len(args.result())))
#
#            if not isinstance(kwargs, Future):
#                print("kwargs keys : {}".format(list(kwargs.keys())))
#            else:
#                print("kwargs keys")
#                print("kwargs keys : {}".format((kwargs.result())))

            if isinstance(args, Future) or \
                    isinstance(kwargs, Future):
                # do computation remotely
                # self.etc should be okay (if not, change to staticmethod)
                res = client.submit(_select_from_mapping, args,
                                    kwargs, *mapping)
                # can't just get elements from tuple, need to submit to cluster

                def get_args(res):
                    return res[0]

                def get_kwargs(res):
                    return res[1]
                args = client.submit(get_args, res)
                kwargs = client.submit(get_kwargs, res)
            else:
                args, kwargs = _select_from_mapping(args, kwargs, *mapping)

            # from SciStreams.globals import debugcache
            # debugcache.append(args)
            # debugcache.append(kwargs)

            sdoc = StreamDoc(self)
            sdoc['args'] = args
            sdoc['kwargs'] = kwargs

            return sdoc
        except Exception:
            statistics = dict()
            _cleanexit(self.select, statistics)
            new_sdoc = StreamDoc(attributes=self['attributes'])
            new_sdoc['statistics'] = statistics
            new_sdoc['_StreamDoc_Type'] = 'error'
            return new_sdoc


# static methods
def _get_return(args, kwargs, elem=None):
    ''' get what the function would have normally returned.

        returns raw data
        Parameters
        ----------
        elem : optional
            if an integer: get that nth argumen
            if a string : get that kwarg
    '''
    if isinstance(elem, str):
        # print("elem is  a str : {}".format(elem))
        res = kwargs[elem]
    elif elem is None:
        # print("elem is None")
        # return general expected function output
        if len(kwargs) > 0:
            res = dict(kwargs)
        elif len(args) > 0 and len(kwargs) == 0:
            res = args
            if len(res) == 1:
                # a function with one arg normally returns this way
                res = res[0]
        else:
            # if it's more complex, then it wasn't a function output
            # make a dictionary This isn't currently used
            # res = kwargs.copy()
            # for i, arg in enumerate(args):
            # key = "_arg{:02d}".format(i)
            # res[key] = arg
            raise ValueError("Error : Did not understand the input")
    else:
        raise ValueError("elem not understood : {}".format(elem))

    return res


def _select_from_mapping(args, kwargs, *mapping):
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
    # streamdoc = StreamDoc(self)
    newargs = list()
    newkwargs = dict()
    totargs = dict(args=newargs, kwargs=newkwargs)
    # quick fix but could be cleaned up
    sdoc = dict(args=args, kwargs=kwargs)

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
           oldkey not in sdoc[oldparentkey] \
           or oldparentkey == 'args' and \
           len(sdoc[oldparentkey]) < oldkey:
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
            totargs[newparentkey].append(sdoc[oldparentkey][oldkey])
        else:
            totargs[newparentkey][newkey] = sdoc[oldparentkey][oldkey]

    # streamdoc['args'] = totargs['args']
    # streamdoc['kwargs'] = totargs['kwargs']

    # this is to make it friendly with distributed
    # return the things that could be futures, then make a StreamDoc from
    # it
    # print("tot args :{}".format(totargs['args']))
    # print("tot kwargs :{}".format(totargs['kwargs']))
    return totargs['args'], totargs['kwargs']


def _is_streamdoc(doc):
    if isinstance(doc, dict) and '_StreamDoc' in doc:
        return True
    else:
        return False


def _is_empty(doc):
    if doc['_StreamDoc_Type'] == 'empty' or\
            doc['_StreamDoc_Type'] == 'error':
        return True
    else:
        return False


def parse_streamdoc(name, filter=False):
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
            def update_kwargs(old_kwargs, in_kwargs, empty=False):
                if not empty:
                    new_kwargs = old_kwargs.copy()
                    new_kwargs.update(in_kwargs)
                    return new_kwargs
                else:
                    return {}

            # add a time out to f
            # TODO : replace with custom time out per stream
            # NOTE : removed timeout for dask implementation
            # f_timeout = timeout(seconds=DEFAULT_TIMEOUT)(f)
            # f_timeout = f
            # print("Running in {}".format(f.__name__))
            # TODO : normalize this more
            prev_stats = dict(cumulative_time=0.)
            # the default
            sdoc_type = 'full'
            sdoc2_type = None
            if x2 is None:
                # this is for map
                if _is_streamdoc(x):
                    prev_stats['cumulative_time'] = \
                        x['statistics']['cumulative_time']
                    sdoc_type = x['_StreamDoc_Type']
                    sdoc2_type = None
                    # extract the args and kwargs
                    args = x.args
                    kwargs = x.kwargs
                    attributes = x.attributes
                else:
                    args = (x,)
                    kwargs = dict()
                    attributes = dict()
            else:
                # this is for accumulate
                if _is_streamdoc(x) and _is_streamdoc(x2):
                    prev_stats['cumulative_time'] = \
                        x['statistics']['cumulative_time']
                    prev_stats['cumulative_time'] += \
                        x2['statistics']['cumulative_time']
                    sdoc_type = x['_StreamDoc_Type']
                    sdoc2_type = x2['_StreamDoc_Type']
                    args = x.get_return(), x2.get_return()
                    # print("found an accumulator. args are {}".format(args))
                    kwargs = dict()
                    # check if attributes are a future
                    if isinstance(x.attributes, Future) or \
                            isinstance(x2.attributes, Future):
                        attributes = client.submit(update_kwargs, x.attributes,
                                                   x2.attributes)
                    else:
                        attributes = x.attributes
                        # attributes of x2 overrides x
                        attributes.update(x2.attributes)
                else:
                    raise ValueError("Two normal arguments not accepted")

            def empty_sdoc(x1, x2):
                ''' Trick to propagate empty values.'''
                # a few cases, x1 exists but not x2 and is full, or both exist
                # and both full
                if (x1 == 'full' and x2 is None) or \
                        (x1 == 'full' and x2 == 'full'):
                    empty = False
                else:
                    empty = True

                return empty

            if remote:
                sdoc_empty = client.submit(empty_sdoc, sdoc_type, sdoc2_type)
            else:
                if isinstance(sdoc_type, Future):
                    sdoc_type = sdoc_type.result()

                if isinstance(sdoc2_type, Future):
                    sdoc2_type = sdoc2_type.result()

                sdoc_empty = empty_sdoc(sdoc_type, sdoc2_type)

            # kwargs is a Future so we need to be careful
            # print(kwargs)
            # print(kwargs_additional)
            if remote:
                kwargs_future = client.submit(update_kwargs, kwargs,
                                              kwargs_additional,
                                              empty=sdoc_empty)
                # print("Sent to cluster")
            else:
                # we don't want to run on cluster
                # check if they're Futures first and turn them to concrete data
                # if yes
                if isinstance(kwargs, Future):
                    kwargs = kwargs.result()
                if isinstance(kwargs_additional, Future):
                    kwargs_additional = kwargs_additional.result()

                kwargs_future = update_kwargs(kwargs, kwargs_additional,
                                              empty=sdoc_empty)

            @wraps(f)
            def unwrap(f, args, kwargs, empty=False):
                # at this stage it's assumed cluster has retrieved kwargs
                # print("unwrapping args {}".format(args))
                # print("unwrapping kwargs {}".format(kwargs))
                # bypass function (avoid long computation times)
                if not empty:
                    return f(*args, **kwargs)
                else:
                    return []

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

            fnew = partial(unwrap, f)
            # re-define f again...
            if remote:
                fnew = future_wrapper(fnew)
                # print("submitting to cluster")
            else:
                # leave as is
                # print("Not submitting to cluster")
                pass

            statistics = dict(prev_stats)
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
            statistics['runtime'] = t2 - t1
            statistics['runstart'] = t1
            statistics['cumulative_time'] += statistics['runtime']

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
            streamdoc = StreamDoc(attributes=attributes, sdoc_type=sdoc_type)
            streamdoc['statistics'] = statistics
            # load in attributes
            # Save outputs to StreamDoc
            # parse arguments to an object with args and kwargs members
            # NOTE : Changed API. Need to ALWAYS assume dict
            # arguments_obj = parse_args(result)

            # print("StreamDoc, parse_streamdoc : parsed args :
            # {}".format(arguments_obj.args))
            # streamdoc.add(args=arguments_obj.args,
            #               kwargs=arguments_obj.kwargs)
            # overly complicated... But works for now...
            # one solution is to force everything to be a dict, instead of
            # allowing args (will have to worry about accumulator for that)
            @wraps(f)
            def get_kwargs(res):
                if isinstance(res, dict):
                    return res
                else:
                    return {}

            @wraps(f)
            def get_args(res):
                if not isinstance(res, dict):
                    return [res]
                else:
                    return []

            # TODO : filter is not being used, should use or delete later?
            if not filter:
                if remote:
                    kwargs = client.submit(get_kwargs, result)
                    args = client.submit(get_args, result)
                    # print("Computation key : {}".format(result.key))
                    # print("Computation status : {}".format(result.status))
                    # print("Computation : {}".format(f.__name__))
                else:
                    kwargs = get_kwargs(result)
                    args = get_args(result)
                streamdoc.add(kwargs=kwargs, args=args)
            else:
                # for filter, we pass old streamdoc but change it's state
                # if filter predicate is True (state can also be Future)
                # make a new StreamDoc
                streamdoc = StreamDoc(x)

                def parse_predicate(sdoc_type, result):
                    if result is True and sdoc_type == 'full':
                        return 'full'
                    else:
                        return 'empty'

                if remote:
                    result = client.submit(parse_predicate, sdoc_type, result)
                else:
                    result = parse_predicate(sdoc_type, result)

                streamdoc['_StreamDoc_Type'] = result

            return streamdoc

        return f_new

    return streamdoc_dec


parse_streamdoc_map = parse_streamdoc("map")
psdm = parse_streamdoc_map
parse_streamdoc_acc = parse_streamdoc("acc")
psda = parse_streamdoc_acc
parse_streamdoc_filter = parse_streamdoc("filter", filter=True)
psdf = parse_streamdoc_filter


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
