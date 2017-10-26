# imports external code
# can't publish this in github (private)

'''
 Main code:
  normalize_img : normalizes the image for the neural networks
  reduce_img : reduces the image to the size necessary for the neural
      networks
  inference_function : the inference funciton

  infer : runs inference which is basically the three functions above:
        normalize_img
        reduce_img
        inference_function

        and then chooses the highest probability tag from the
            logits outputted from the inference function
'''


from ..config import config

from functools import partial, wraps
from sidl.nn_fbbenet.infer import infer as sidl_infer  # noqa
from sidl.nn_fbbenet.infer import normalize_img, reduce_img  # noqa
from sidl.nn_fbbenet.infer import inference_function as sidl_inffunc  # noqa

checkpoint_filename = config.get('modules', {})\
    .get('tensorflow', {}).get('checkpoint_filename', None)

infer = partial(sidl_infer, checkpoint_filename=checkpoint_filename)
infer = wraps(sidl_infer)(infer)
inference_function = partial(sidl_inffunc,
                             checkpoint_filename=checkpoint_filename)
inference_function = wraps(sidl_inffunc)(inference_function)
