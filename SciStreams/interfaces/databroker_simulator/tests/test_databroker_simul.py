from SciStreams.interfaces.databroker_simulator\
        .databroker_simulator import pullfromuid


def test_databroker_simulator():
    sdoc = pullfromuid("foo", dbname="bar")

    assert 'kwargs' in sdoc
    assert 'attributes' in sdoc

    assert 'pilatus300' in sdoc['kwargs']

    attrs = sdoc['attributes']

    keys = [
            'experiment_alias_directory',
            'detector_name',
            'sample_savename',
            'scan_id',
            'detector_SAXS_x0_pix',
            'detector_SAXS_y0_pix',
            ]

    for key in keys:
        assert key in attrs
