# dictionaries
# detector info at:
# http://gisaxs.com/CS/index.php/CMS#Detectors

# Singlet is a dict pretty much, but it enforces a value and unit
from SciAnalysis.data.Singlet import Singlet

# very explicit, wordy, but better
detectors2D = {
    'pilatus300' : 
            dict(
                pixel_size_x =          Singlet(value=172, unit="um"),
                pixel_size_y =          Singlet(name='pixel_size_y', value=172, unit="um"),
                shape =                 Singlet(value=(619, 487), unit="pixel"),
                dynamic_range =         Singlet(value=20, unit="bit"),
                readout_time =          Singlet(value=7, unit="ms"),
                point_spread_function = Singlet(value=1, unit="pixel"),
                max_frame_rate =        Singlet(value= 20, unit="Hz"),
                description =           Singlet(value='SAXS detector',unit=None),
                # the detector keys
                image_key =             Singlet(value='pilatus300_image', unit=None),
                stats1_key =            Singlet(value='pilatus300_stats1_total', unit=None),
                stats2_key =            Singlet(value= 'pilatus300_stats2_total', unit=None),
                stats3_key =            Singlet(value= 'pilatus300_stats3_total', unit=None),
                stats4_key =            Singlet(value= 'pilatus300_stats4_total', unit=None),
            ),
    'PhotonicSciences_CMS' : 
            dict(
                shape  =                Singlet(value= (1042, 1042), unit="pixel"),
                pixel_size_x =          Singlet(value= 101.7, unit="um"),
                pixel_size_y =          Singlet(value= 101.7, unit="um"),
                dynamic_range =         Singlet(value= 16, unit="bit"),
                ),
}
