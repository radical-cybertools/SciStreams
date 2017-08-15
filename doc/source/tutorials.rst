.. currentmodule::
  tutorials

Stream Tutorials
================
The following tutorials are meant to walk through a user on how to use
streams.  At the end of these tutorials, you should know enough to take
any pre-written function and insert into the CMS pipeline, which follows
this stream based logic.

Install Instructions
--------------------
Before testing, it's recommended you start a conda analysis environment
and install the necessary packages. You can this locally with the
following instructions. For now, they're a little long. The main reason is that
we need the latest versions of ``dask`` ``distributed`` and ``scikit-beam`` for
the streams to work effectively (some pull requests were specifically made to
``dask`` and ``distributed`` recently to get this to work)::

  conda create -n testenv2 pip nose python=3.6 numpy=1.11 coverage cython
  flake8 scipy
  source activate testenv2
  cd projects # some folder
  git clone https://github.com/CFN-softbio/SciStreams.git
  cd SciStreams
  python setup.py develop
  pip install codecov cachey pillow matplotlib distributed dask pyyaml
  ipython
  pushd ..
  git clone http://www.github.com/scikit-beam/scikit-beam.git
  cd scikit-beam
  python setup.py develop
  popd
  pushd ..
  git clone http://www.github.com/dask/dask.git
  cd dask
  python setup.py develop
  popd
  pushd ..
  git clone http://www.github.com/dask/distributed.git
  cd distributed
  python setup.py develop
  popd

Tutorial 1 : Simple Data Stream
-------------------------------

The core of streams is the ``Stream`` object, developed by `Matt Rocklin
<http://www.github.com/mrocklin/streams>`_. Let's start with a simple
function computation.  Thinking about streams is a little different from
usual computations.  The biggest change from a regular python script is
that the computations are separated from the data.

For example, here is a setup of a simple stream, which takes in a
number, multiplies it by two, and finally prints the result to screen::

  # This originally comes from http://www.github.com/mrocklin/streams
  from SciStreams.interfaces.streams import Stream
  
  # functions
  def mtimes2(a): 
      return 2*a 
   
  # set up the computations
  sin = Stream() 
  s2 =  sin.map(mtimes2) 
  sout = s2.map(print) 
   
  # the data is sent through 'emit' (loop over this for incoming data)
  sin.emit(1) 

Note the nice abstraction between data and function, compared to the old
method, which would have been::

  def mtimes2(a):
      return 2*a
  
  # less lines but this can get messsy
  print(mtimes2(1))
 
Using streams involves more lines of code, however, it allows for more
complex computations, as will be seen eventually.

Tutorial 2 : Multiple Inputs
----------------------------

Before moving on to more complex stream operations, let us first show
how to deal with functions with multiple inputs.
Previously, the function ``mtimes2`` only took one input so it was
simple to ``map`` onto.
For functions with multiple inputs/outputs, we need to develop some sort
of ``wrapper`` that basically encodes the inputs for a function in a
single entity which can be decoded (or transformed) back into
conventional outputs. This is resolved in two steps:

1. We need a way to normalize data. This is done with an object called
``StreamDoc``. It is defined as follows::

  from SciStreams.interfaces.StreamDoc import StreamDoc
  # define a StreamDoc
  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))

The ``StreamDoc`` declaration means encode an input which is meant to be
``f(2, 3, c=1)`` for some function ``f`` and save it into the variable
``sdoc``. 

2. We also need a way to parse the data in such a way that its contents
are mapped as inputs to functions (without having to re-write a new
function that handles the data specifically). This is done with the
``psdm`` wrapper::

  def multiply(a,b, c=1): 
      return a*b*c

  from SciStreams.interfaces.StreamDoc import psdm
  new_multiply = psdm(multiply)

  sdoc_result = new_multiply(sdoc)


This results in a new ``StreamDoc`` that looks as such::

  print(sdoc_result)

  {'_StreamDoc': 'StreamDoc v1.0',
   'args': [6],
   'attributes': {'function_list': ['multiply']},
   'kwargs': {},
   'statistics': {},
   'uid': '77781a1e-0d05-49c2-8eed-529aa0e3b19a'}

This ``psdm`` wrapper (short for ``parse_streamdoc_map``) allows the
mapping of the ``StreamDoc``'s contents into the appropriate inputs of
the function.

The combination of this new data format (the ``StreamDoc``) and an
encoder/decoder (``parse_streamdoc``) for this new data format allows
one to handle multiple inputs.


Finally, we can tie this into streams again by simply ``mapping`` the
new function, and the ``StreamDoc`` generated::

  from SciStreams.interfaces.streams import Stream
  from SciStreams.interfaces.StreamDoc import StreamDoc
  # wrapper for the StreamDoc
  from SciStreams.interfaces.StreamDoc import psdm
  
  def multiply(a,b, c=1): 
      return a*b*c
  
  sin = Stream()
  s2 =  sin.map(psdm(multiply))
  sout = s2.map(psdm(print))
  
  
  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))
  sin.emit(sdoc)

Here, we have the same sort of ``map`` logic. However, the new function
mapped onto, takes three arguments as input. Two are unnamed arguments
and one is an optional keyword argument. How do we encode this arguments
into a singleton of data that we can then pass on into a stream?

The answer is with the ``StreamDoc``. In this case the following::

  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))


This can be understood by the following schematic:

#[[File:Pipeline_stream_operations_map.png|300px]]


where ``SDin`` and ``SDout`` are incoming/outgoing ``StreamDoc``
and the transformation and its inverse **T/T^(-1)** are taken
care of by the ``psdm`` wrapper.

Tutorial 3 : Re-mapping inputs/outputs
--------------------------------------

Mapping inputs and outputs is nice. However, if one reads the previous
examples, one will notice that such mapping is done before the entry
point (``emit``) by instantiating a ``StreamDoc``. There is no
freedom to change inputs/outputs as we progress in the stream. One could
imagine this would be a useful option to have.

How does one go about this? The answer is simple, through the
``select`` option. Let us work through a simple example. Let's say we
wanted to run the following function::

  def multiplyadd(a,b, c=1):
      return a*(b+c)

with as input the following ``StreamDoc``::

  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))

Let's say that we actually wanted a to be the second arg of the
StreamDoc, and b to be the first. Remapping this would be simple through
the ``select`` method::

  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))

  sin = Stream()
  # swap arg1 and arg2
  # This will make 3 first arg and 2 second
  s2 = sin.select(1, 0, 'c')
  s3 =  s2.map(psdm(multiplyadd))
  sout = s3.map(psdm(print))
  
  sin.emit(sdoc)

Tutorial 4 : Mapping Args into keyword args and vice versa
----------------------------------------------------------

Now, let's exend our simple example to more combinations of mappings.
Let's suppose now that our function does not contain keyword arguments
at all::

  # now c is not kwarg but is kwarg in streamdoc
  def multiplyadd(a, b, c):
      return a*(b+c)

and again our StreamDoc was::

  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))

Re-mapping is as easy as specifying a tuple, where the first element is
the key name (or arg number), and the second element is either another
key name, or ``None`` (to specify that it is an arg)::

  from SciStreams.interfaces.StreamDoc import select

  sin = Stream()
  # swap arg1 and arg2
  s2 = sin.map(select, 1, 0, ('c',None))
  s3 =  s2.map(psdm(multiply))
  sout = s3.map(psdm(print))

  # now 3 is first arg and 2 is second
  sdoc = StreamDoc(args=(2,3), kwargs=dict(c=1))
  sin.emit(sdoc)

For more details on this, please see the [[Pipeline:Main:SelectionRules|
Selection Rules]].

Tutorial 5 : Merging Streams
----------------------------

Finally, how would one merge streams? It is easy. Let's say we have
three streams which will receive as input the following three
StreamDocs::

  sdoc1 = StreamDoc(args=(1,2), kwargs=dict(a=1, b=2, c=3))
  sdoc2 = StreamDoc(args=(3,6), kwargs=dict(a=4, c=3, d=1))
  sdoc3 = StreamDoc(kwargs=dict(e=1))

Let's say we wanted to combine them all together. Combining them is as
simple as instantiaing three streams and calling ``merge``::

  from SciStreams.interfaces.StreamDoc import merge

  # instantiate the three streams for sdoc1, sdoc2, and sdoc3,
  respectively
  sin1 = Stream()
  sin2 = Stream()
  sin3 = Stream()
  
  # merge sin1 with sin2 and sin3
  s2 = sin1.zip(sin2, sin3).map(merge)
  # this is also equivalent to:
  #s2 = sin1.zip(sin2.map(select,0,1,'a','c', 'd'), sin3.map(select,
  'e')).map(merge)
  
  # if we now select different elements:
  s3 = s2.map(select, 0,1) # should give (1,2)
  s4 = s2.map(select, 2,3) # should give (3,6)
  s5 = s2.map(select, 'a','e') # should give (a=4,e=1)
  
  s5.map(print)
  
  # The actual sending of data is really done here:
  sin1.emit(sdoc1)
  sin2.emit(sdoc2)
  sin3.emit(sdoc3)

The merging rules are straightforward. All args are appended from left
to right, in the order the streams are written. Any colliding kwargs are
overidden with the rightermost stream that last contained it. For
example, ``a`` occurs in both ``sdoc1`` and ``sdoc2``. The final value
of ``a`` after the merge will be that from ``sdoc2``, since it is the
rightermost stream ``(a=4)``.

Note that the print from the s5 stream only occurs after **all** the
documents have been emitted. This is controlled by ``zip``.

Tutorial 6 : Adding Attributes (Metadata)
-----------------------------------------

At CMS, all measurements carry metadata along with the data. This
metadata allows us to keep track of many things, such as who took the
measurement, what kind of sample was involved, sample beamstop position
etc. Lots of this information is essential to identifying the data, and
its resulting computations, but not essential for atomistic
computations.

Let's look at the following example. Let's say that we wanted to mask an
image, and then look at the counts at the beam center.::

  from SciStreams.interfaces.StreamDoc import merge, select,
  get_attributes, psdm, StreamDoc
  from SciStreams.interfaces.streams import Stream
  import numpy as np
  
  def multiply(img, mask):
      return img*mask
  
  def ctsatcenter(img, center):
      return img[center]
  
  
  sin = Stream()
  s2 = sin.map(select, ('image', None), ('mask',
  None)).map(psdm(multiply))
  s_attr = sin.map(get_attributes).map(select, ('beam_origin', None))
  s3 = s2.zip(s_attr).map(merge)
  s4 = s3.map(psdm(ctsatcenter))
  s4.map(psdm(print)) # sink to printing the result
  
  img1 = np.random.random((100,100))
  mask1 = np.ones((100,100))
  sdoc = StreamDoc(kwargs=(dict(image=img1, mask=mask1)),
  attributes=dict(name="Bob", beam_origin=(10,10)))
  sin.emit(sdoc)

Here in s2, the image is multiplied by the mask, and the attributes
passed through. However, for the next computation, the beam center is
needed. It's extracted using ``get_attributes``, merged in and then
mapped to the function that extracts this information
(``ctsatcenter``).

Note there is a general rule for attributes. They are always passed
through, and for a ``merge`` operation, they follow the same conflict
rules that ``kwargs`` follow.

Tutorial 7 : Submitting to Cluster
----------------------------------
It is possible to submit some computations to a distributed scheduler
and have them gathered back.

::

     # some global variables
     from SciStreams.interfaces.streams import Stream
     from SciStreams.interfaces.StreamDoc import StreamDoc

     from distributed import Client
     client = Client()

     def inc(x):
        print("Incrementing")
        return x + 1

     s = Stream()
     s2 = s.map(lambda x : client.submit(inc, x))
     s2 = s2.map(lambda x : client.gather(x))

     s.emit(3)
     s.emit(3)
     s.emit(4)

There are two things happening here:

1. The results are being sent to the cluster through ``client.submit``.
   The returned result will now be a ``dask.Future`` instead of the data
   itself. A ``dask.Future`` object is very simple. It contains a
   reference to the data on the cluster. For some future ``f =
   Future()``, retrieving the result is then as simple as calling ``res
   = f.result()``.
2. The results are collected via ``client.gather()``. This basically
   calls the ``.result()`` member of each incoming ``dask.Future`` object in
   the stream, turning it into a concrete result once again.


Advanced Tutorial 8 : Submitting to Cluster and Caching
-------------------------------------------------------
Sometimes redundant computations are sent to the cluster. It would be
nice to have these computations cached so that they aren't computed over
and over again. This is especially true when computing large 2D ``q`` and
``phi`` maps. There is an easy mechanism to do so using
``dask.distributed``. More information can be found `here
<http://distributed.readthedocs.io/en/latest/manage-computation.html>`_.

The basic idea is that the distributed scheduler will cache all results
in memory that have a valid ``Future`` pointing to it.
Here is an example:

::

     # some global variables
     import SciStreams.globals as s_globals
     from SciStreams.interfaces.streams import Stream
     from SciStreams.interfaces.StreamDoc import StreamDoc

     from distributed import Client
     client = Client()

     def inc(x):
        print("Incrementing")
        return x + 1

     s = Stream()
     s2 = s.map(lambda x : client.submit(inc, x))
     # sink the resultant futures to list
     s2.map(s_globals.futures_cache.append)
     s2 = s2.map(lambda x : client.gather(x))

     s.emit(3)
     s.emit(3)
     s.emit(4)


The only difference in this case is that now, before the stream is
``gather`` ed, its ``Future`` is also sent off to a queue
``s_globals.futures_cache``. This queue is actually a global queue
supplied by ``SciStreams`` for convenience. It has a maximum length
defined by ``MAX_FUTURE_NUM`` in ``SciStreams.globals``.
