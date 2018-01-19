import argparse
import re
import time
import matplotlib
matplotlib.use("Agg")  # noqa

from SciStreams.startup import run_stream

VERSION = "0.2"

re_time_pairs = [
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), "%Y-%m-%d"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}$"), "%Y-%m-%d %H"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}" +
                "[0-9]{2}:[0-9]{2}$"), "%Y-%m-%d %H:%M"),
    (re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}" +
                "[0-9]{2}:[0-9]{2}:[0-9]{2}$"), "%Y-%m-%d %H:%M:%S"),
]


def check_time(strtime):
    for rexp, strform in re_time_pairs:
        if rexp.match(strtime):
            tstruct = time.strptime(strtime, strform)
            return time.mktime(tstruct)
    errormsg = "Error, time not understood\n"
    errormsg += "Formats accepted: \n"

    for r, t in re_time_pairs:
        errormsg += "{}\n".format(t)

    raise ValueError(errormsg)


if __name__ == '__main__':
    print("CMS pipeline, version {}".format(VERSION))

    parser = argparse.ArgumentParser(description='Start the pipeline')
    parser.add_argument('-t0', '--start_time', dest='start_time', type=str)
    parser.add_argument('--stop_time', dest='stop_time', type=str)
    parser.add_argument('--test', dest='test', type=str)
    parser.add_argument('--maxrun', dest='maxrun', type=str)
    args = parser.parse_args()
    if isinstance(args.test, str) and args.test.lower() == 'true':
        print("test mode, testing one uid")
        uids = ['3e5742d3-4e11-43d0-abe8-af6e44c26bf2']
        run_stream.start_run(uids=uids)
        print("Sleeping for an hour... (hit CTRL+C to quit)")
        time.sleep(3600)
    else:
        # help="The start time for the pipeline " +
        # "(default is 24 hrs prior to now)")
        if args.start_time is not None:
            # will raise a ValueError
            start_time = check_time(args.start_time)
        else:
            # 1 day earlier
            start_time = time.time()-24*3600
        stop_time = args.stop_time

        print("Searching for results " +
              "from {} onwards...".format(time.ctime(start_time)))
        # run pipeline
        if hasattr(args, 'maxrun'):
            maxrun = int(args.maxrun)
        else:
            maxrun = None
        run_stream.start_run(start_time, stop_time=stop_time, maxrun=maxrun)
