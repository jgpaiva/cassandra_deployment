"""Get statistics

Usage:
  stats.py
  stats.py --operations

Options:
  --operations  Calculate per-node operations
"""

from glob import glob
from docopt import docopt

from environment import numeric_const_pattern

names = ['replication']

dirs = glob("results.*")

def operations():
    for directory in dirs:
        try:
            relevant_settings = _settings(directory)
            operations = list(sorted(_operations(directory)))

            print("%s\t%s\t%s" % (relevant_settings,operations,directory))
        except IOError:
            print "Could not read files for %s" % directory

def throughput():
    for directory in dirs:
        try:
            relevant_settings = _settings(directory)

            try:
                throughput = sum(_throughputs(directory))
            except:
                throughput = "N/A"

            try:
                print("%s\t%.0f %s" % (relevant_settings,throughput,directory))
            except TypeError:
                print("%s\t%s %s" % (relevant_settings,throughput,directory))
        except IOError:
            print "Could not read files for %s" % directory

def _throughputs(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            thrp_strs = filter(lambda x: "Throughput" in x,f)
        yield get_last_number(thrp_strs[0])

def _operations(directory):
    for filename in glob(directory+"/*/AllReads.log"):
        with open(filename,'r') as f:
            operations = sum(int(get_last_number(line)) for line in f if "=" in line)
        if operations > 0:
            yield operations

def _settings(directory):
    with open(directory + "/settings.out", 'r') as f:
        settings = eval(f.read())

    keys = filter(lambda x: any(name in x for name in names), settings)
    relevant_settings = ", ".join(
        "%s:%s" % (x, settings[x]) for x in keys)
    return relevant_settings

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return float(val[-1])

if __name__ == '__main__':
    arguments = docopt(__doc__, version='1.0')
    if arguments["--operations"]:
        operations()
    else:
        throughput()
