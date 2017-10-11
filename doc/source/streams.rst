.. currentmodule::
  streams

Streams
=======
The streams work from two main abstractions.

The Stream
----------

When running computations, there are a number of approaches that may be taken.
Since computation pipelines can run complex very quickly, we try to modularize
as much as we can. We decide to then take a streaming approach, which abstracts
away the input/output data from the computations. We use the new `streamz
library <https://github.com/mrocklin/streamz>`_.

Since the library is fairly new, we have constructed upon it a framework that
meets our needs. 

A general workflow would look as follows::

  # import the streams library
  from streamz import Stream
  # import the necessary tools to parse the incoming data
  from SciStreams.core.StreamDoc import StreamDoc
  import SciStreams.core.scistreams as scs

  # define the stream here
  sin = Stream()
  # use arbitrary function get_attributes to extract data
  sout = scs.get_attributes(sin)
  # apply a scientific function circavg to data
  # but first wrap it with a parser, psdm in order to map inputs/outputs
  # (explained later)
  sout = scs.map(circavg, sout)
  # finally add saving
  sout.sink(save_to_file)
  # etc...

  # finally, send the data through the stream
  for record in data:
    sin.emit(record)

The details are more thoroughly explained in the :doc:`Tutorials
<tutorials>` section.

The StreamDoc
-------------

For streams to function, some convention of the inputs/outputs to the
streams must be agreed upon. A simple convention is assuming that
streams pass just one entity to functions etc. This is not enough for
us.

We choose to use a ``StreamDoc``, which is basically a dictionary with
the following four elements:

1. ``args`` : this is an ordered tuple of outputs from a stream. This
   becomes the ``args`` of any mapping onto a function and hence the
   name.

2. ``kwargs`` : this is a dictionary of outputs from a stream. This
   becomes the ``kwargs`` of any mapping onto a function and hence the
   name.

3. ``attributes`` : This is a dictionary of meta-data that must be
passed through.

4. ``statistics`` : This is a dictionary of run-specific meta-data that
   must be passed through. This differs from ``attributes`` in that
   this data is generally stream-specific, not data specific. (Ex :
   runtime, errors returned etc...)

At first, this sort of convention may seem a little cumbersome, but it's
actually quite natural. An analogue to this is a network packet. When
data is sent across a network, it must be encapsulated by multiple
layers of headers for each layer in the processing stream to correctly
understand what to do with the data.
