#test a stream that uses the tokenizing attribute
from SciAnalysis.analyses.XSAnalysis import Data
from SciAnalysis.interfaces.streams import Stream
from SciAnalysis.interfaces.StreamDoc import StreamDoc, delayed_wrapper
from SciAnalysis.interfaces.StreamDoc import make_delayed_stream_dec


#@make_delayed_stream_dec("stream")
def gen_qxyz(obj):
    obj._generate_qxyz_maps()
    return obj

#@make_delayed_stream_dec("stream")
def get_qxmap(obj):
    return obj.qx_map_data

from dask import compute as dask_compute
def compute(obj):
    return dask_compute(obj)[0]

s = Stream(wrapper=delayed_wrapper)
sout = s.map(gen_qxyz)
sout.apply(print)
#sout.apply(compute).apply(print)
s.map(get_qxmap).apply(print)
#s.apply(get_qxmap).apply(compute).apply(print)


# wavelenght(Angs), dist(m), pixel size(microns)
calib = Data.Calibration(1., 5., 75e-6)
calib.set_image_size(100, 100)
calib.set_beam_position(50, 50)
sdoc = StreamDoc(args=[calib])
print(sdoc.tokenize())
s.emit(StreamDoc(args=[calib]))
print("")

calib = Data.Calibration(1., 5., 75e-6)
calib.set_image_size(100, 100)
calib.set_beam_position(50, 50)
s.emit(StreamDoc(args=[calib]))
print("")

calib = Data.Calibration(1., 5., 78e-6)
calib.set_image_size(100, 100)
calib.set_beam_position(50, 50)
s.emit(StreamDoc(args=[calib]))
print("")
