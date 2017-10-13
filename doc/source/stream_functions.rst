Various Streams
===============
Various streams have been pre-created in ``SciStreams``. Here is the list of
current existing streams.
The procedure for adding these streams is generally::

  from streamz import Stream
  from SciStreams.streams.XS_Streams import CircularAverageStream
  s_main = Stream()
  sin_circavg, sout_circavg = CircularAverageStream()
  # now sin_circavg downstream of s_main
  s_main.connect(sin_circavg)
  # finally, do something for downstream data, for ex, save to list
  L = sout_circavg.sink_to_list()

  # emit data as usual
  for sdoc in sdocs:
    s_main.emit(sdoc)

See the examples in the docstring each stream creator for more details.


.. currentmodule:: SciStreams.streams.XS_Streams

.. autosummary::
   PrimaryFilteringStream
   AttributeNormalizingStream
   CalibrationStream
   CircularAverageStream
   QPHIMapStream
   LineCutStream
   CollapseStream
   AngularCorrelatorStream
   ImageStitchingStream
   ThumbStream
   PeakFindingStream
   ImageTaggingStream
   PCAStream

.. autofunction:: PrimaryFilteringStream
.. autofunction:: AttributeNormalizingStream
.. autofunction:: CalibrationStream
.. autofunction:: CircularAverageStream
.. autofunction:: QPHIMapStream
.. autofunction:: LineCutStream
.. autofunction:: CollapseStream
.. autofunction:: AngularCorrelatorStream
.. autofunction:: ImageStitchingStream
.. autofunction:: ThumbStream
.. autofunction:: PeakFindingStream
.. autofunction:: ImageTaggingStream
.. autofunction:: PCAStream
