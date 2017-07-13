class Singlet(dict):
    ''' Just a dict of number
        This is equivalent to dict(value=213, unit='pixel') etc
        This just helps better enforce restrictions
    '''
    def __init__(self, name, value=None, unit=None):
        super(Singlet, self).__init__({name: dict(value=value, unit=unit)})
