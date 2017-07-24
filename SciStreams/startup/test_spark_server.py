from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

from SciAnalysis.interfaces.databroker import pullfromuid

def get_calib(sdoc):
    class Calib:
        pass
    calib = Calib()
    attr = sdoc['attributes']
    calib.origin = attr['detector_SAXS_x0_pix'], attr['detector_SAXS_y0_pix']
    return calib

# this would pull a sciresult
data = lines.map(pullfromuid)
calib = data.map(get_calib)
