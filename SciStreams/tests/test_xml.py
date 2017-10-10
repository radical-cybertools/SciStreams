from SciStreams.interfaces.xml.xml import sdoc_to_xml
from SciStreams.core.StreamDoc import StreamDoc

def test_sdoc():
    ''' Just make sure it converts fine'''
    attributes = dict(name='foo',
                        sample_savename='bar')
    kwargs = dict(a=1)
    args = [1, 'foo2']
    sdoc = StreamDoc(attributes=attributes,
                        kwargs=kwargs,
                        args=args)

    root = sdoc_to_xml(sdoc)
    return root
