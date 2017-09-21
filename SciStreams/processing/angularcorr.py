from rdeltaphicorr.rdpc import RDeltaPhiCorrelator

def angular_corr(image, mask=None, origin=None, bins=(800,360)):
    '''
        image : the image to process
        beamx0, beamy0 : the beam center
        bins : number of q phi bins to process into
    '''
    print('bins {}'.format(bins))
    rdpc = RDeltaPhiCorrelator(image.shape, mask=mask, origin=origin,
                               rbins=bins[0], phibins=bins[1])
    rdpc.run(image)
    # return the normalized delta phi correlatin
    res = dict(
            rdeltaphiavg = rdpc.rdeltaphiavg,
            rdeltaphiavg_n = rdpc.rdeltaphiavg_n,
            )
    return res
