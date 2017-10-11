from SciStreams.config import validate_md


def test_validate_md():
    ''' Test it runs with some data we expect to test.
        Note validation is created from external yml file
    '''
    vdict = dict(
        # name, type
        detector_SAXS_x0_pix="number",
        detector_SAXS_y0_pix="number",
        motor_SAXSx="number",
        motor_SAXSy="number",
        scan_id="int",
        measurement_type="str",
        sample_savename="str",
        sample_name="str",
        motor_bsx="number",
        motor_bsy="number",
        motor_bsphi="number",
        detector_SAXS_distance_m="number",
        calibration_energy_keV="number",
        calibration_wavelength_A="number",
        experiment_alias_directory="str",
        experiment_cycle="str",
        experiment_group="str",
        filename="str",
        sample_exposure_time="number",
        stitchback="int",
    )
    md = dict(sample_name="test",
              motor_bsx=-15.17,
              motor_bsy=-16.9,
              motor_bsphi=-12,
              motor_SAXSx=-65,
              motor_SAXSy=-72.,
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

    validate_md(md, validate_dict=vdict)

    md2 = md.copy()
    md2.pop('sample_name')
