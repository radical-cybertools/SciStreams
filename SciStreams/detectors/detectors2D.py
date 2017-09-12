import yaml
import os.path

# this may be cludgy, I need to read a yaml file within the directory
# I prefer to save as yml than make a dict in the event we want to override
# this in the future
import SciStreams
filename = SciStreams.__file__

dirname = os.path.dirname(filename) + "/detectors"

f = open(dirname + "/detectors2D.yml")
detectors2D = yaml.load(f)
