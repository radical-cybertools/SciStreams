
# functional approach
#databroker_keymap = {'infile' : 'pilatus300_image'}
@delayed(pure=True)
@parse_sciresults({'infile' : 'infile'})
def load_saxs_image(infile=None, detector=None, database=None):
    if isinstance(infile, Header):
        if detector is None:
            raise ValueError("Sorry, detector must be passed if supplying a header")
        if database is None:
            raise ValueError("Sorry, database must be passed if supplying a header")
        img = database.get_images(header, detector['image_key']['value'])[0]
        img = np.array(img)
    elif isinstance(infile, np.ndarray):
        img = infile
    elif isinstance(infile, str):
        img = np.array(Image.open(infile))
    else:
        raise ValueError("Sorry, did not understand the input argument: {}".format(infile))
    results = SciResult(image = np.array(img))

    return results

_keymap = {'calib' : 'calib'}
@delayed(pure=True)
@parse_sciresults(_keymap)
def load_calibration(calib, **kwargs):
    '''
        Load calibration data.
        The data must be a dictionary.
        either:
            load_calibration(calib=myCalib)
        or:
            load_calibration(wavelength=fdsa) etc
        
        It is curried so you can keep overriding parameters.


        This is area detector specific.
    '''
    # defaults of function
    _defaults= {'wavelength' : {'value' : None, 'unit' : 'Angstrom'},
                 'beamx0' : {'value' : None, 'unit' : 'pixel'},
                 'beamy0' : {'value' : None, 'unit' : 'pixel'},
                 'sample_det_distance' : {'value' : None, 'unit' : 'm'},
                # Area detector specific entries:
                 # width is columns, height is rows
                 'AD_width' : {'value' : None, 'unit' : 'pixel'},
                 'AD_height' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_x' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_y' : {'value' : None, 'unit' : 'pixel'},

                }

    if isinstance(calib, Header):
        # a map from Header start doc to data
        # TODO : move out of function
        calib_keymap = {'wavelength' : {'key' : 'calibration_wavelength_A',
                                        'unit' : 'Angstrom'},
                        'detectors' : {'key' : 'detectors',
                                        'unit' : 'N/A'},
                        'beamx0' : {'key' : 'detector_SAXS_x0_pix', 
                                    'unit' : 'pixel'},
                        'beamy0' : {'key' : 'detector_SAXS_y0_pix',
                                    'unit' : 'pixel'},
                        'sample_det_distance' : {'key' : 'detector_SAXS_distance_m',
                                                 'unit' : 'pixel'}
                        }

        start_doc = calib['start']
        calib_tmp = dict()
        # walk through defaults
        for key, entry in calib_keymap.items():
            start_key = entry['key'] # get name of key
            unit = entry['unit']
            val = start_doc.get(start_key, _defaults[key]['value'])
            calib_tmp[key] = {'value' : val,
                              'unit' : unit}

        # finally, get the width and height by looking at first detector in header
        # TODO : add ability to read more than one detector, maybe in calib_keymap
        first_detector = start_doc[calib_keymap['detectors']['key']][0]
        detector_key = detectors2D[first_detector]['image_key']['value']

        # look up in local library
        pixel_size_x = detectors2D[first_detector]['pixel_size_x']['value']
        pixel_size_x_unit = detectors2D[first_detector]['pixel_size_x']['unit']
        pixel_size_y = detectors2D[first_detector]['pixel_size_y']['value']
        piyel_size_y_unit = detectors2D[first_detector]['piyel_size_y']['unit']

        img_shape = detectors2D[first_detector]['shape']

        calib_tmp['pixel_size_x'] = dict(value=pixel_size_x, unit=pixel_size_x_unit)
        calib_tmp['pixel_size_y'] = dict(value=pixel_size_y, unit=pixel_size_y_unit)
        calib_tmp['shape'] = dict(value=img_shape, unit='pixel')
        calib = calib_tmp
    
    # now set defaults, if not set
    for key, val in _defaults.items():
        if key not in calib:
            calib[key] = _defaults[key]

    # returns a nested dictionary
    return SciResults(calibration=calibration)

_keymap = {'calibration' : 'calibration', 'image' : 'image', 'mask' : 'mask'}
@delayed(pure=True)
@parse_sciresults(_keymap)
def circular_average(image=None, calibration=None, bins=100, mask=None):
    #print(calibration)
    x0, y0 = calibration['beamx0'], calibration['beamy0']
    from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic
    rbinstat = RadialBinnedStatistic(calibration['shape']['value'], bins=bins, origin=(y0,x0), mask=mask)
    sq = rbinstat(image)
    sqx = rbinstat.bin_centers
    return SciResult(sqx=sqx, sqy=sq)

