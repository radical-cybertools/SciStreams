.. currentmodule::
  visualizing_streams

Visualizing Streams
===================

Visualizing Streams
-------------------
The ``SciStreams`` library inherits everything that the ``streamz``
library offers. Visualizing a stream is as simple as using the
``visualize`` command (note: you need the ``graphviz`` library installed
for this to work)::

  import streamz
  import SciStreams.core.scistreams as scs

  def myfunc(a, b):
    return a + b

  s = streamz.Stream()
  s2 = scs.map(myfunc, s)
  s3 = s2.sink(print)

  s.visualize('mystream.png')

Here the ``visualize`` class method will output a visualization of the
stream to ``mystream.png``.

.. image:: figs/mystream.png
