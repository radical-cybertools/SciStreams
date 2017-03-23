from SciAnalysis.SciResult import SciResult
# This decorator parses SciResult objects, indexes properly
# takes a keymap for args
# this unravels into arguments if necessary
# TODO : Allow nested keymaps
# NOTE : I used to check with 'isinstance(val, SciResult)'
#       This can lead to problems. If SciResult is always the result of
#       it being imported, that's okay. However, if it's defined in the same file,
#       it will be a different instance. This is too dangerous, so I am ignoring it now.
# TODO : explain this also cleans all other parameters not specified by output
def parse_sciresults(keymap, output_names):
    # from keymap, make the decorator
    def decorator(f):
        # from function modify args, kwargs before computing
        def _f(*args, **kwargs):
            for i, val in enumerate(args):
                if isinstance(val, dict) and '_SciResult' in val:
                #if isinstance(val, SciResult):
                    key = "_arg{}".format(i)
                    args[i] = val[keymap[key]]
            for key, val in kwargs.items():
                if isinstance(val, dict) and '_SciResult' in val:
                #if isinstance(val, SciResult):
                    kwargs[key] = val[keymap[key]]
            resultdict = kwargs.copy()
            for key, val in list(resultdict.items()):
                if not key.startswith("_"):
                    resultdict.pop(key)
            result = f(*args, **kwargs)
            if len(output_names) == 1:
                resultdict.update({output_names[0] : result})
            else:
                resultdict.update({output_names[i] : res for i, res in enumerate(result)})
            # this is so databroker can know how to reproduce function result
            resultdict['_output_names'] = output_names

            return SciResult(**resultdict)
        return _f
    return decorator
