import argparse
import re
import time
from .startup import run_stream_live

VERSION = "0.1"

re_time_pairs = [
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), "%Y-%d-%m"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}$"), "%Y-%d-%m %H"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}" +
                "[0-9]{2}:[0-9]{2}$"), "%Y-%d-%m %H:%M"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}" +
                "[0-9]{2}:[0-9]{2}:[0-9]{2}$"), "%Y-%d-%m %H:%M:%S"),
]


def check_time(strtime):
    for rexp, strform in re_time_pairs:
        if rexp.match(strtime):
            tstruct = time.strptime(strtime, strform)
            return time.mktime(tstruct)
    errormsg = "Error, time not understood\n"
    errormsg += "Formats accepted: {}\n"

    for r, t in re_time_pairs:
        errormsg += "{}\n".format(t)

    raise ValueError(errormsg)


if __name__ == '__main__':
    print("CMS pipeline, version {}".format(VERSION))

    parser = argparse.ArgumentParser(description='Start the pipeline')
    parser.add_argument('-t0', '--start_time', dest='start_time', type=str)
    # help="The start time for the pipeline " +
    # "(default is 24 hrs prior to now)")
    args = parser.parse_args()
    if args.start_time is not None:
        # will raise a ValueError
        start_time = check_time(args.start_time)
    else:
        # 1 day earlier
        start_time = time.time()-24*3600

    print("Searching for results " +
          "from {} onwards...".format(time.ctime(start_time)))
    # run pipeline
    run_stream_live.start_run(start_time)
