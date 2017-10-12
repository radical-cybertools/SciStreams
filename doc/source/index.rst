.. SciAnalysis documentation master file, created by
   sphinx-quickstart on Tue Jul 11 17:42:28 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to SciAnalysis's documentation!
=======================================

.. toctree::
 :maxdepth: 2
 :caption: Contents:

 installation
 streams
 tutorials
 debugging
 api

.. currentmodule::
  intro


Motivation
----------

At CMS, we are faced with the following challenges:

1. We need to retrieve/save data from multiple sources. 

  * Ex: databroker, xml, file, plotting, etc

2. We need to cache/store/retrieve intermediate results. 

  * Ex : Running an angular correlation from a 2D detector image, or computing
    a 1D curve from a 2D detector image. Both computations would require
    information about the coordinate system on the detector.

3. As we chain computations, we need to pass important metadata (attributes)
   that better describe the computation we're doing. We consider these
   attributes to be independent of the computations but crucial for
   post-computation review. 

  * Example : Pass the *sample_name* from the loading of an image all the way
    down to a stored 1D curve. This parameter is non-essential to the
    computation of the 1D curve.

  * Example 2 : Pass the *sample_name* and other tags, from loading/masking to
    a machine learning algorithm classifying this data set. During
    classification, sending tags is not necessary but helps later determine the
    accuracy of the classification.

4. We need to distribute the jobs across multiple machines to speed up computation time.

These problems are resolved with the following abstractions:

1. The ``Stream`` object : Computations are first constructed as a graph.
   This helps abstract away three components : the **computations**, the
   ``source`` data and finally the ``sinks`` (where the data will end up
   going). What is nice about this abstraction is that moving from a local to
   distributed regime is as simple as wrapping the computation functions with a
   decorator. This helps satisfy condition 2 (using sinks) and condition 3
   (chaining through a graph).
2. The **StreamDoc** object : For normalizing inputs/outputs, some sort of
   assumption on the inputs/outputs must be made. It is currently made with the
   **StreamDoc** object. This will be explained later. This object helps
   satisfy condition 1.

