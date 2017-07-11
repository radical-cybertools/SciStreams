from SciAnalysis.interfaces.streams import Stream, NOOP

#Schrodinger's stream

s = Stream()

s1 = s.filter(lambda x : True)
s1.map(lambda x : print("in true branch"), raw=True)

s2 = s.filter(lambda x : False)
s2.map(lambda x : print("should not print")).map(lambda x : print("in false branch"), raw=True)

s.emit(2)
