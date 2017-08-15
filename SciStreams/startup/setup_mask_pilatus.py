from SciStreams.data.Mask import MaskFrame, BeamstopXYPhi, MaskGenerator
import numpy as np
from SciStreams.config import config, masks
from PIL import Image
masks_config = masks

# the detector key obtained from databroker
det_key = 'pilatus300_image'

from SciStreams.interfaces.databroker.databases import databases
cddb = databases['cms:data']

uid = "d1728ef6-9ad0-487e-94d8-254b50a4bb61"

hdr = cddb[uid]
start_time = "2017-07-12"
stop_time = "2017-07-14"
hdrs_GISAXS = list(cddb(sample_name="Julien_beamstop_GISAXS_2", start_time=start_time, stop_time=stop_time))
hdrs_SAXS = list(cddb(sample_name="Julien_beamstop_SAXS_2", start_time=start_time, stop_time=stop_time))
#hdr = hdrs_GISAXS[11]
hdr = hdrs_SAXS[0]
# TODO : allow multiple detectors
det_key = hdr['start']['detectors'][0] + "_image"

pilatus_mask_config = masks_config[det_key]

bstop_kwargs = pilatus_mask_config['beamstop']
frame_kwargs = pilatus_mask_config['frame']
blemish_kwargs = pilatus_mask_config['blemish']

mask_filename = config['maskdir'] + "/" + det_key + "/pilatus300_mask_master_beamstop.npz"

from functools import partial
#bstop_kwargs['ref_rotx'] = 581
#bstop_kwargs['ref_roty'] = 187
beamstop_pilatus = partial(BeamstopXYPhi, **bstop_kwargs)
frame_pilatus = MaskFrame(**frame_kwargs)



# initialize according to parameters from databroker
#bstop_GISAXS = beamstop_pilatus(bsphi=-223.4, bsx=-16.55, bsy=17.00)
#bstop_SAXS = beamstop_pilatus(bsphi=-12, bsx=-16.43, bsy=-14.787)

bsphi = hdr['start']['motor_bsphi']
bsx= hdr['start']['motor_bsx']
bsy= hdr['start']['motor_bsy']

img = cddb.get_images(hdr, det_key)[0]
obstruction = beamstop_pilatus(bsphi=bsphi, bsx=bsx,bsy=bsy) + frame_pilatus
blemish = np.array(Image.open(blemish_kwargs['filename']))
if blemish.ndim == 3:
    blemish = blemish[:,:,0]

mmg = MaskGenerator(obstruction=obstruction, blemish=blemish)
x0 = hdr['start']['detector_SAXS_x0_pix']
y0 = hdr['start']['detector_SAXS_y0_pix']
origin = y0, x0

mask = mmg.generate(origin)

from pylab import *
ion()
figure(0);clf();
imshow(obstruction.mask)
plot(obstruction.origin[1], obstruction.origin[0], 'ro')

figure(2);clf()
imshow(img*(mask +.1));clim(0,10)
