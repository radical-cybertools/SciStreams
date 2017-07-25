from .stream import Stream


# Experimental: goal : to better define input/output checking
class StreamGroup:
    ''' A group of streams. An input and output format must be specified.

        # Formats should be of type
        image, mask, a=, g=
    '''
    def __init__(self, inputs, outputs, name=None, input_formats=None):
        if name is None:
            name = "None"
        self.inputs = inputs
        self.outputs = outputs
        self.input_format=input_format

    def emit(self, x):
        for input in inputs:
            input.emit(x)

    def __repr__(self):
        msg = "A {} stream\n".format(self.name)
        if self.input_formats is not None:
            msg += "input formats: \n"

            for i, input_format in enumerate(input_formats):
                msg += "input {} : {}\n".format(i, input_format)

            msg += "input formats: \n"
            for i, input_format in enumerate(input_formats):
                msg += "input {} : {}\n".format(i, input_format)

        if self.output_formats is not None:
            msg += "output formats: \n"

            for i, output_format in enumerate(output_formats):
                msg += "output {} : {}\n".format(i, output_format)

            msg += "output formats: \n"
            for i, output_format in enumerate(output_formats):
                msg += "output {} : {}\n".format(i, output_format)

        return msg

    def _validate_input_formats(self):
        # strip white space, tokenize with ","
        # strings ending with "=" are kwargs, otherwise args
        pass
