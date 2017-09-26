from SciStreams.core.StreamDoc import StreamDoc

def test_streamdoc():

    # test initialization
    data = dict(a=1, b=2)
    sdoc = StreamDoc(data=data)
    assert sdoc['data']['a'] == 1

    # update from another sdoc
    sdoc2 = StreamDoc(streamdoc=sdoc)
    assert sdoc2['data']['a'] == 1
