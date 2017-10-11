.. currentmodule::
 tips 

Tips
====

Stream Structure
----------------
If some stream behaviour is yielding unexpected results, a last resort is to
look at the various tests for the modules. These are generally a good way to
see how the streams have been used succesfully. If no such test exists for your
expected behaviour, then it is suggested to add one.

Callbacks
---------
Various callbacks are used to output results from streams into various forms of
data. Here are some tips for some of them.

hdf5 callbacks
~~~~~~~~~~~~~~
 * When saving data to hdf5 callbacks, make sure to save large arrays as numpy
   arrays (``np.ndarray``), and not lists. Lists will be saved as separate
   elements in the hdf5 file whereas arrays are stored as compact arrays. The
   main reason lists are not saved the same are that the elements cannot be
   assured to be homogeneous.

