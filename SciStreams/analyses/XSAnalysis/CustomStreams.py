#try a partially filled lattice
import numpy as np
from ...interfaces.StreamDoc import StreamDoc, Arguments
from ...interfaces.streams import Stream
from dask import compute, delayed

# wrappers for parsing streamdocs
from ...interfaces.StreamDoc import select, pack, unpack, todict,\
        add_attributes, psdm, psda



# TODO : make this part of streams
# Sample custom stream : fit to a sphere form factor

def SqFitStream(wrapper=None):
    ''' Create a stream for S(q) fit.
        Inputs :
            'sqx'
            'sqy'

        Outputs:
            'sqx'
            'sqy'
            'sqfit'
            'parameters'

    '''
    sin = Stream()
    s2 = sin.map(add_attributes, stream_name="SqFitCustom")
    s3 = s2.map(select,('sqx', None), ('sqy', None))
    s4 = s3.map(psdm(fitsqsphere))
    sout = s4
    return sin, sout




def fitsqsphere(q, sqdata, Ncutoff=None):
    ''' This will fit to a sphere structure factor.
        The weights are weighted towards high q (q^4)
        output:
            dictionary :
                parameters : the parameters
                sqx : the q values
                sqy : the data
                sqfit : the fit to the data
    '''
    from ScatterSim.NanoObjects import SphereNanoObject, PolydisperseNanoObject
    from lmfit import Model, Parameters

    if Ncutoff is None:
        Ncutoff = 0

    # the function
    def calc_sphereff(q, radius, sigma_R, A, B):
        pargs = {'radius' : radius, 'sigma_R' : sigma_R}
        sphere = PolydisperseNanoObject(SphereNanoObject, pargs=pargs, argname='radius', argstdname='sigma_R')
        sqmodel = sphere.form_factor_squared_isotropic(q)
        return A*sqmodel + B

    # using lmfit
    #radius = 4.41;sigma_R = radius*.08
    params = Parameters()
    params.add('radius', value=4.41)
    params.add('sigma_R', value=0.08)
    params.add('A', value=1e-4, min=0)
    params.add('B', value=1, min=0)

    model = Model(calc_sphereff)
    res = model.fit(sqdata, q=q, params=params, weights=q**4)
    best_fit = res.best_fit

    return Arguments(parameters=res.best_values, sqx=q, sqy=sqdata, sqfit=best_fit)



# TODO :  need to fix this
@delayed
def squash(sdocs):
    newsdoc = StreamDoc()
    for sdoc in sdocs:
        newsdoc.add(attributes = sdoc['attributes'])
    N = len(sdocs)
    cnt = 0
    newargs = []
    newkwargs = dict()
    for sdoc in sdocs:
        args, kwargs = sdoc['args'], sdoc['kwargs']
        for i, arg in enumerate(args):
            if cnt == 0:
                if isinstance(arg, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(arg.shape)
                    newargs.append(np.zeros(newshape))
                else:
                    newargs.append([])
            if isinstance(arg, np.ndarray):
                newargs[i][cnt] = arg
            else:
                newargs[i].append[arg]

        for key, val in kwargs.items():
            if cnt == 0:
                if isinstance(val, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(val.shape)
                    newkwargs[key] = np.zeros(newshape)
                else:
                    newkwargs[key] = []
            if isinstance(val, np.ndarray):
                newkwargs[key][cnt] = val
            else:
                newkwargs[key].append[val]

        cnt = cnt + 1

    newsdoc.add(args=newargs, kwargs=newkwargs)

    return newsdoc

