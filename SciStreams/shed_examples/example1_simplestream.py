# simple stream
# works as defined by streams
#increment by one then print
from streams import Stream
import streams.core

s = Stream()
s2 = s.map(lambda x : x + 1)
s3 = s2.map(print)

# send data by emitting
# etc
s.emit(1)
s.emit(10)
s.emit(20)

# an alternative way to do this is the following, 
# Calling the core method and 
# passing the previous stream as an argument
# Note the subtle difference, this is important to follow
# for the next examples. we may interchange between both methods
# double check that both yield the same output
s = Stream()
s2 = streams.core.map(lambda x : x + 1, s)
s3 = streams.core.map(print, s2)

# send data by emitting
# etc
s.emit(1)
s.emit(10)
s.emit(20)
