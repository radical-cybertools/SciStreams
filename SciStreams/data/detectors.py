# dictionaries
# detector info at:
# http://gisaxs.com/CS/index.php/CMS#Detectors

# Singlet is a dict pretty much, but it enforces a value and unit
from ..data.Singlet import Singlet
from collections import ChainMap
# TODO maybe have members accessed with obj.pixel_size_x.value etc
# TODO : generalize and move out? (make independent of beamline?)

# very explicit, wordy, but better
detectors2D = {
    'pilatus300':
        ChainMap(
            Singlet('pixel_size_x', value=172, unit="um"),
            Singlet('pixel_size_y', value=172, unit="um"),
            Singlet('shape', value=(619, 487), unit="pixel"),
            Singlet('dynamic_range', value=20, unit="bit"),
            Singlet('readout_time', value=7, unit="ms"),
            Singlet('point_spread_function', value=1, unit="pixel"),
            Singlet('max_frame_rate', value=20, unit="Hz"),
            Singlet('description', value='SAXS detector', unit=None),
            # the detector keys
            Singlet('image_key', value='pilatus300_image', unit=None),
            Singlet('stats1_key', value='pilatus300_stats1_total', unit=None),
            Singlet('stats2_key', value='pilatus300_stats2_total', unit=None),
            Singlet('stats3_key', value='pilatus300_stats3_total', unit=None),
            Singlet('stats4_key', value='pilatus300_stats4_total', unit=None),
        ),
    'PhotonicSciences_CMS':
        ChainMap(
            Singlet('shape', value=(1042, 1042), unit="pixel"),
            Singlet('pixel_size_x', value=101.7, unit="um"),
            Singlet('pixel_size_y', value=101.7, unit="um"),
            Singlet('dynamic_range', value=16, unit="bit"),
        ),
}
