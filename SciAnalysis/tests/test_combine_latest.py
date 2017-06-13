from SciAnalysis.interfaces.streams import Stream

s1 = Stream()
s2 = Stream()
sout = s1.combine_latest(s2)
sout.map(print)

s2.emit(1)
s1.emit(1)
s2.emit(1)
s1.emit(1)
s2.emit(1)
s1.emit(1)
s2.emit(1)
s1.emit(1)

#s1.emit(1)
#s2.emit(1)

