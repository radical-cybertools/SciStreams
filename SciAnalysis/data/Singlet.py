class Singlet(dict):
    ''' Just a dict of number
        This is equivalent to dict(value=213, unit='pixel') etc
        This just helps better enforce restrictions
    '''
    def __init__(self, name=None, value=None, unit=None):
        super(Singlet, self).__init__(value=value, unit=unit)
        if name is not None:
            self['name'] = name
