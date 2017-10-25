from ..simulators.saxs import mkSAXS
from ..simulators.gisaxs import mkGISAXS
from ..detectors.detectors2D import detectors2D
from ..tests.simulators import generate_event_stream

def stream_generator(savepath="/tmp/unnamed_analysis"):
    ''' Generate a stream according to CMS' data.
        This is just for convenience.
    '''
    print("Creating simulated stream")
    x0, y0 = 743, 581.
    detx, dety = -65, -72
    peaks = [40, 80, 100, 120, 200, 300, 400, 700, 1000, 1300, 1500, 2000,
             2500, 2600]
    peakamps = [.0003]*len(peaks)
    sigma = 6.

    md = dict(sample_name="test",
              motor_bsx=-15.17,
              motor_bsy=-16.9,
              motor_bsphi=-12,
              # these get filled in in loop
              # motor_SAXSx = -65,
              # motor_SAXSy = -72.,
              # detector_SAXS_x0_pix=x0,
              # detector_SAXS_y0_pix=y0,
              # scan_id=0,
              detector_SAXS_distance_m=5.,
              calibration_energy_keV=13.5,
              calibration_wavelength_A=0.9184,
              experiment_alias_directory=savepath,
              experiment_cycle="2017_3",
              experiment_group="SciStream-test",
              filename="foo.tiff",
              # updated every time
              # sample_savename="out",
              sample_exposure_time=10.,
              stitchback=True)

    shape = detectors2D['pilatus2M']['shape']['value']
    scl = detectors2D['pilatus2M']['pixel_size_x']['value']

    stream = list()
    shiftsx = [-6, 0, 6]
    shiftsy = [-8, 0, 8]
    scan_id = 0
    sym = 6  # 2*np.int(np.random.random()*12)
    phase = 2*np.pi*np.random.random()
    for shiftx in shiftsx:
        for shifty in shiftsy:
            x1 = x0 - shiftx*scl
            y1 = y0 - shifty*scl
            detx1, dety1 = detx+shiftx, dety+shifty

            md = md.copy()
            md.update(detector_SAXS_x0_pix=x1)
            md.update(detector_SAXS_y0_pix=y1)
            md.update(motor_SAXSx=detx1)
            md.update(motor_SAXSy=dety1)
            md.update(scan_id=scan_id)
            md.update(measurement_type="SAXS")
            md.update(sample_savename="sample_x{}_y{}".format(detx1, dety1))

            data = mkSAXS(shape, peaks, peakamps, phase, x1, y1, sigma, sym)

            plt.figure(1)
            plt.clf()
            plt.imshow(data)
            plt.pause(.1)

            data_dict = dict(pilatus2M_image=data)
            stream.extend(generate_event_stream(data_dict,
                                                md=md))
            scan_id += 1

    # try some GISAXS patterns
    shiftx, shifty = 0, 0
    x1 = x0 - shiftx*scl
    y1 = y0 - shifty*scl
    detx1, dety1 = detx+shiftx, dety+shifty
    r = 3
    ld = 12
    Narray = 5
    md = md.copy()
    md.update(measurement_type="GISAXS")
    md.update(stitchback=False)
    data = mkGISAXS(shape, r, ld, Narray, x1, y1)
    data_dict = dict(pilatus2M_image=data)

    stream.extend(generate_event_stream(data_dict,
                                        md=md))
    return stream
