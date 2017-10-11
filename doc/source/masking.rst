.. currentmodule::
  masking 

Masking Data
============
Often the incoming data needs to be masked. Due to how routine this is,
data masking is incorporated into ``SciStreams`` directly.

Masking Data
------------
Often acquired data will contain regions that need to be masked. These
regions often change very little from run cycle to run cycle. An example
in ``SciStreams.examples.mask_creation`` is included to show how one
could both add a mask and use it. 

The main idea is to:
 1. Acquire and possibly stitch image data togehter if needed
 2. Make a mask from the acquired data (either using a python routine or
     image editing software on a saved raw image).
 3. Load the mask back in.
 4. Save the mask as an ``.npz`` file with all relevant attributes that
     describe it.

The masks are then generated with a ``MaskGenerator`` object. This mask
generator looks in a directory of masks for the mask that matches the
detector type and the specified required attrbutes (ex: motor positions)
for a certain tolerance.

The instructions are still a work in progress.
