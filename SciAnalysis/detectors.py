# dictionaries
# detector info at:
# http://gisaxs.com/CS/index.php/CMS#Detectors
detectors2D = {
    'pilatus300' : {'xpixel_size_um' : 172,
                    'aspect_ratio_xoy' : 1., # x/y aspect ratio
                    'shape' : (487, 619),
                    'dynamic_range_bits' : 20,
                    'readout_time_ms' : 7,
                    'point_spread_function_pixel' : 1,
                    'max_frame_rate_Hz' : 20,
                    'description' : 'SAXS detector',
                    # the detector keys
                    'image_key' : 'pilatus300_image',
                    'stats1_key' : 'pilatus300_stats1_total',
                    'stats2_key' : 'pilatus300_stats2_total',
                    'stats3_key' : 'pilatus300_stats3_total',
                    'stats4_key' : 'pilatus300_stats4_total',
                    },
    'PhotonicSciences_CMS' : {
                    'shape' : (1042, 1042),
                    'xpixel_size_um' : 101.7,
                    'aspect_ratio_xoy' : 1.,
                    'dynamic_range_bits' : 16,
                    },
}
