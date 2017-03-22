# dictionaries
# detector info at:
# http://gisaxs.com/CS/index.php/CMS#Detectors


# very explicit, wordy, but better
detectors2D = {
    'pilatus300' : 
            dict(
                pixel_size_x =          dict(value=172, unit="um"),
                pixel_size_y =          dict(name='pixel_size_y', value=172, unit="um"),
                shape =                 dict(value=(619, 487), unit="pixel"),
                dynamic_range =         dict(value=20, unit="bit"),
                readout_time =          dict(value=7, unit="ms"),
                point_spread_function = dict(value=1, unit="pixel"),
                max_frame_rate =        dict(value= 20, unit="Hz"),
                description =           dict(value='SAXS detector',unit=None),
                # the detector keys
                image_key =             dict(value='pilatus300_image', unit=None),
                stats1_key =            dict(value='pilatus300_stats1_total', unit=None),
                stats2_key =            dict(value= 'pilatus300_stats2_total', unit=None),
                stats3_key =            dict(value= 'pilatus300_stats3_total', unit=None),
                stats4_key =            dict(value= 'pilatus300_stats4_total', unit=None),
            ),
    'PhotonicSciences_CMS' : 
            dict(
                shapei =                dict(value= (1042, 1042), unit="pixel"),
                pixel_size_x =          dict(value= 101.7, unit="um"),
                pixel_size_y =          dict(value= 101.7, unit="um"),
                dynamic_range =         dict(value= 16, unit="bit"),
                ),
}
