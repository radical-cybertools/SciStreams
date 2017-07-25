# image stitching
import numpy as np


def roundbydigits(n, digits=3):
    ''' round by the number of digits.
        n can be an array

        0, nan or inf are just passed through
    '''
    if n is None:
        return None

    if isinstance(n, np.ndarray):
        w = np.where(~np.isinf(n)*~np.isnan(n)*(n != 0))
        n_new = np.copy(n)
        if len(w[0]) > 0:
            sign = (n[w] > 0)*2 - 1
            power = np.round(np.log10(n[w]*sign), decimals=0).astype(int)
            n_new[w] = np.round(n[w]/10**power, decimals=digits-1)
            n_new[w] = n_new[w]*(10**power)
    else:
        if n == 0 or np.isinf(n) or np.isnan(n):
            n_new = n
        else:
            sign = (n > 0)*2 - 1
            power = np.round(np.log10(n*sign), decimals=0)
            power = int(power)
            n_new = np.round(n/10**power, decimals=digits-1)
            n_new = n_new*(10**power)

    return n_new




def xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc):
    ''' Stitch_acc may not be necessary, it should just be a binary flag.  But
            could be generalized to a sequence number so I leave it.
    '''
    mask_acc = mask_acc.astype(int)
    # need to make a copy
    img_acc_old = img_acc
    img_acc = np.zeros_like(img_acc_old)
    w = np.where(mask_acc != 0)
    img_acc[w] = img_acc_old[w]/mask_acc[w]
    mask_acc = (mask_acc > 0).astype(int)

    return dict(image=img_acc, mask=mask_acc, origin=origin_acc, stitchback=stitchback_acc)

def xystitch_accumulate(prevstate, newstate):
    '''
        (assumes IMG and mask np arrays)
        prevstate : IMG, mask, (x0, y0), stitchback
        nextstate : incoming IMG, mask, (x0, y0), stitchback

        rules for stitchback:
            if next state is True, stitch from previous image
            else:
                re-initialize
            NOTE : The stitchback parameter MUST be True to stitch
                Values such as 1, etc will not work.

        returns accumulated state

        Assumes calibration object has sample exposure time

        NOTE : x is cols, y is rows
            where img[rows][cols]
            so shapey, shapex = img.shape

        NOTE #2 : Assumes all incoming images have the same size

        TODO : Do we want subpixel accuracy stitches?
            (this requires interpolating pixels, maybe not
                good for shot noise limited regime)
    '''
    # If state lost, reset and recover
    if len(prevstate) != 4:
        # print("Prevstate 0, returning newstate")
        return newstate

    img_next, mask_next, origin_next, stitchback_next = newstate
    # just in case
    img_next = img_next*(mask_next > 0).astype(int)
    shape_next = img_next.shape

    # logic for making new state
    # initialization:
    # make backwards compatible
    # only works for True, 0, 1 will be false
    if stitchback_next is not True:
        # re-initialize
        img_acc = img_next.copy()
        shape_acc = img_acc.shape
        mask_acc = mask_next.copy()
        origin_acc = origin_next
        stitchback_acc = False
        return img_acc, mask_acc, origin_acc, stitchback_acc

    # else, stitch
    img_acc, mask_acc, origin_acc, stitchback_acc = prevstate
    shape_acc = img_acc.shape

    # logic for main iteration component
    # NOTE : In matplotlib, bottom and top are flipped (until plotting in
    # matrix convention), this is logically consistent here but not global
    bounds_acc = _getbounds2D(origin_acc, shape_acc)
    bounds_next = _getbounds2D(origin_next, shape_next)
    # check if image will fit in stitched image
    expandby = _getexpansion2D(bounds_acc, bounds_next)
    #print("need to expand by {}".format(expandby))

    img_acc = _expand2D(img_acc, expandby)
    mask_acc = _expand2D(mask_acc, expandby)
    #print("New shape : {}".format(img_acc.shape))

    origin_acc = origin_acc[0] + expandby[2], origin_acc[1] + expandby[0]
    _placeimg2D(img_next, origin_next, img_acc, origin_acc)
    _placeimg2D(mask_next, origin_next, mask_acc, origin_acc)

    stitchback_acc = stitchback_next
    img_acc = img_acc*(mask_acc > 0)
    mask_acc = mask_acc*(mask_acc > 0)
    newstate = img_acc, mask_acc, origin_acc, stitchback_acc

    return newstate

def _placeimg2D(img_source, origin_source, img_dest, origin_dest):
    ''' place source image into dest image. use the origins for
    registration.'''
    bounds_image = _getbounds2D(origin_source, img_source.shape)
    left_bound = origin_dest[1] + bounds_image[0]
    low_bound = origin_dest[0] + bounds_image[2]
    low_bound = int(low_bound)
    left_bound = int(left_bound)
    img_dest[low_bound:low_bound+img_source.shape[0],
             left_bound:left_bound+img_source.shape[1]] += img_source

def _getbounds(center, width):
    return -center, width-1-center

def _getbounds2D(origin, shape):
    # NOTE : arrays index img[y][x] but I choose this way
    # because convention is in plotting, cols (x) is x axis
    yleft, yright = _getbounds(origin[0], shape[0])
    xleft, xright = _getbounds(origin[1], shape[1])
    return [xleft, xright, yleft, yright]

def _getexpansion(bounds_acc, bounds_next):
    expandby = [0, 0]
    # image accumulator does not extend far enough left
    if bounds_acc[0] > bounds_next[0]:
        expandby[0] = bounds_acc[0] - bounds_next[0]+1

    # image accumulator does not extend far enough right
    if bounds_acc[1] < bounds_next[1]:
        expandby[1] = bounds_next[1] - bounds_acc[1]+1

    return expandby

def _getexpansion2D(bounds_acc, bounds_next):
    expandby = list()
    expandby.extend(_getexpansion(bounds_acc[0:2], bounds_next[0:2]))
    expandby.extend(_getexpansion(bounds_acc[2:4], bounds_next[2:4]))
    return expandby

def _expand2D(img, expandby):
    ''' expand image by the expansion requirements. '''
    if not any(expandby):
        return img

    expandby = np.array(expandby).astype(int)
    dcols = expandby[0] + expandby[1]
    drows = expandby[2] + expandby[3]

    img_tmp = np.zeros((img.shape[0] + drows, img.shape[1] + dcols))
    img_tmp[expandby[2]:expandby[2]+img.shape[0], expandby[0]:expandby[0]+img.shape[1]] = img

    return img_tmp
