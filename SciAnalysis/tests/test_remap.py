
def remap(dict1, map1):
    ''' remap a dictionary to keys specified.
        Only map if the entry exists, otherwise ignore.
    '''
    dict2 = dict()
    for key, newkey in map1.items():
        if isinstance(newkey, str):
            if key in dict1:
                dict2[newkey] = dict1[key]
        elif isinstance(newkey, tuple):
            res = dict1
            for elem in newkey:
                res = res[elem]
            dict2[newkey] = res
    return dict2

a = {"g" : ["g", "gs"], "b" : "2321", "h" : 1}
keymap = {"g" : ("g",0), "b" : "b"}

b = remap(a, keymap)
