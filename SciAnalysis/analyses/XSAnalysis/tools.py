# image stitching
import numpy as np

def xystitch_result(img_acc, mask_acc, origin_acc, stitch_acc):
    ''' Stitch_acc may not be necessary, it should just be a binary flag.  But
            could be generalized to a sequence number so I leave it.
    '''
    from SciAnalysis.globals import tmp_cache
    tmp_cache.put('mask_acc', mask_acc, 10)
    tmp_cache.put('img_acc', img_acc, 10)

    mask_acc = mask_acc.astype(int)
    # need to make a copy
    img_acc_old = img_acc
    img_acc = np.zeros_like(img_acc_old)
    w = np.where(mask_acc != 0)
    img_acc[w] = img_acc_old[w]/mask_acc[w]
    mask_acc = (mask_acc > 0).astype(int)

    return dict(image=img_acc, mask=mask_acc, origin=origin_acc, stitch=stitch_acc)

def xystitch_accumulate(prevstate, newstate):
    '''
        (assumes IMG and mask np arrays)
        prevstate : IMG, mask, (x0, y0), stitch
        nextstate : incoming IMG, mask, (x0, y0), stitch

        rules for stitch:
            if next state 0, re-initialize
            else : stitch from previous image

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

    img_next, mask_next, origin_next, stitch_next = newstate
    # just in case
    img_next = img_next*(mask_next > 0).astype(int)
    shape_next = img_next.shape

    # logic for making new state
    # initialization:
    if stitch_next == 0:
        # re-initialize
        img_acc = img_next.copy()
        shape_acc = img_acc.shape
        mask_acc = mask_next.copy()
        origin_acc = origin_next
        stitch_acc = 0
        return img_acc, mask_acc, origin_acc, stitch_acc

    # else, stitch
    img_acc, mask_acc, origin_acc, stitch_acc = prevstate
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

    stitch_acc = stitch_next
    img_acc = img_acc*(mask_acc > 0)
    mask_acc = mask_acc*(mask_acc > 0)

    return img_acc, mask_acc, origin_acc, stitch_acc

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
        expandby[0] = bounds_acc[0] - bounds_next[0]

    # image accumulator does not extend far enough right
    if bounds_acc[1] < bounds_next[1]:
        expandby[1] = bounds_next[1] - bounds_acc[1]

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
