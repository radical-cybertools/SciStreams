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


from sidl.nn_fbbenet.infer import infer  # noqa
from sidl.nn_fbbenet.infer import normalize_img, reduce_img  # noqa
from sidl.nn_fbbenet.infer import inference_function  # noqa
