import numpy as np


def mkGISAXS(shape, r, ld, Narray, x1, y1):
    ''' make a simulated pattern for GISAXS.

        WARNING: This is a crude pattern.
            This is not a real pattern. It essentially plots two patterns
            separated by some distance (i.e. one can't circular average this
            pattern).
    '''
    # TODO : need to fix, doesn't work for all centers
    # did not have time to think about this exactly yet
    from SciStreams.data.StitchedImage import StitchedImage
    from shapesim.shapes import CubicLattice3Ellipses

    # maxdim = np.max(shape)
    # maxdim = np.max([maxdim//2, maxdim-x1, x1, maxdim-y1, y1])
    maxshape = np.max(shape)
    cenx, ceny = shape[1]//2, shape[0]//2
    dx = np.abs(cenx - x1)
    dy = np.abs(ceny - y1)
    maxdim = np.max([int(2*maxshape), int(2*maxshape + 2*dx),
                     int(2*maxshape + 2*dy), shape[0], shape[1]])
    print('maxdim : {}'.format(maxdim))

    # make a GISAXS pattern, choose some values
    r = 5
    ld = 12
    Narray = 5

    alpha_i = 10
    alphar_i = np.radians(alpha_i)
    # the shift in pixels delta
    delta = 50

    dims = [maxdim, maxdim, maxdim]
    # try the 4 terms in DWBA
    shp_1 = CubicLattice3Ellipses(r, r, 0, ld, Narray, dims=dims)
    shp_1.addunits([0, 0, 0])
    shp_1.project()

    shp_2 = CubicLattice3Ellipses(r, r*np.cos(2*alphar_i), 0, ld, Narray,
                                  dims=dims)
    shp_2.addunits([0, 0, 0])
    shp_2.project()

    shp_3 = CubicLattice3Ellipses(r, r*np.cos(alphar_i), 0, ld, Narray,
                                  dims=dims)
    shp_3.addunits([0, 0, 0])
    shp_3.project()

    shp_4 = CubicLattice3Ellipses(r, r*np.cos(alphar_i), 0, ld, Narray,
                                  dims=dims)
    shp_4.addunits([0, 0, 0])
    shp_4.project()

    img1 = shp_1.fimg2
    img2 = shp_2.fimg2
    img3 = shp_3.fimg2
    # img4 = shp_4.fimg2

    mask = np.ones_like(img1)

    # real_image = shp_1.img.real

    # just add first three terms not fourth
    cen1 = np.array(img1.shape)//2
    simg1 = StitchedImage(img1, cen1)
    mask1 = StitchedImage(mask, cen1)

    cen2 = cen1[0] + delta, cen1[1]
    simg2 = StitchedImage(img2, cen2)
    mask2 = StitchedImage(mask, cen2)

    cen3 = cen1[0] + delta, cen1[1]
    simg3 = StitchedImage(img3, cen3)
    mask3 = StitchedImage(mask, cen3)

    simg = simg1 + simg2 + simg3
    mask = mask1 + mask2 + mask3

    scat = simg.image/mask.image

    newcen = np.array([simg.refpoint[0] + y1-cen1[0],
                       simg.refpoint[1] + x1-cen1[1]]).astype(int)
    # new det origin
    # oo = y1-shape[0]//2, x1-shape[1]//2

    newscat = scat[newcen[0]:newcen[0] + shape[0], newcen[1]:newcen[1] +
                   shape[1]]

    return newscat
