from rdeltaphicorr.rdpc import RDeltaPhiCorrelator


def angular_corr(image, mask=None, origin=None, bins=(800, 360),
                 method='bgsub', r_map=None):
    '''
        image : the image to process
        beamx0, beamy0 : the beam center
        bins : number of q phi bins to process into
        r_map : the q map
    '''
    rdpc = RDeltaPhiCorrelator(image.shape, mask=mask, origin=origin,
                               rbins=bins[0], phibins=bins[1], method=method,
                               r_map=r_map)
    rdpc.run(image)
    # return the normalized delta phi correlatin
    res = dict(
               rdeltaphiavg=rdpc.rdeltaphiavg,
               rdeltaphiavg_n=rdpc.rdeltaphiavg_n,
               qvals=rdpc.rvals,
               phivals=rdpc.phivals,
               )
    return res
