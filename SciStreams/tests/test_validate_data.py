from SciStreams.config import validate_md


def test_validate_md():
    md = dict(sample_name="test",
              motor_bsx=-15.17,
              motor_bsy=-16.9,
              motor_bsphi=-12,
              motor_SAXSx = -65,
              motor_SAXSy = -72.,
              detector_SAXS_x0_pix=0.,
              detector_SAXS_y0_pix=0.,
              scan_id=0,
              detector_SAXS_distance_m=5.,
              calibration_energy_keV=13.5,
              calibration_wavelength_A=0.9184,
              measurement_type="foo",
              experiment_alias_directory="/foo",
              experiment_cycle="2017_3",
              experiment_group="SciStream-test",
              filename="foo.tiff",
              # updated every time
              sample_savename="out",
              sample_exposure_time=10.,
              stitchback=True)

    validate_md(md)

    md2 = md.copy()
    md2.pop('sample_name')

