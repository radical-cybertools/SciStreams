from SciStreams.core.StreamDoc import StreamDoc, to_event_stream
# stuff useful to simulate data coming in

def generate_event_stream(data, md={}):
    # generate some name document pairs
    attrs = md
    kwargs = dict(**data)


    sdoc = StreamDoc(attributes=attrs, kwargs=kwargs)

    event_stream = to_event_stream(sdoc, tolist=True)

    return event_stream
