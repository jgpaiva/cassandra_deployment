"""Get statistics

Usage:
  stats.py
  stats.py --operations
  stats.py --top
  stats.py --check_items
  stats.py --large_repl

Options:
  --operations   Calculate per-node operations
  --check_items  Calculate per-node top items len
  --top          Calculate per-node top items
  --large_repl   Calculate number of large_repl items
"""

from glob import glob
from docopt import docopt

from environment import numeric_const_pattern

names = ['replication','save']

dirs = glob("results.*")


def iterdirs(funct):
    def retval():
        for directory in dirs:
            try:
                funct(directory)
            except IOError:
                print "Could not read files for %s" % directory
    return retval

def printval(funct):
    def retval(directory):
        relevant_settings = _settings(directory)
        res = funct(directory)
        print("%s\t%s\t%s" % (relevant_settings,res,directory))
    return retval

@iterdirs
@printval
def operations(directory):
    return list(sorted(_operations(directory)))

@iterdirs
@printval
def large_repl(directory):
    return list(_large_repl(directory))

@iterdirs
@printval
def top(directory):
    return list(_top(directory,5))

@iterdirs
@printval
def check_items(directory):
    return list(_check_items(directory))

@iterdirs
def throughput(directory):
    relevant_settings = _settings(directory)

    try:
        throughput = sum(_throughputs(directory))
    except:
        throughput = "N/A"

    try:
        print("%s\t%.0f %s" % (relevant_settings,throughput,directory))
    except TypeError:
        print("%s\t%s %s" % (relevant_settings,throughput,directory))

def _throughputs(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            thrp_strs = filter(lambda x: x.startswith("[OV") and "Throughput" in x,f)
            yield get_last_number(thrp_strs[0])

def _large_repl(directory):
    for run_out in glob(directory+"/*/LargeRepl*"):
        with open(run_out,'r') as f:
            line = f.read()
            yield len(line.split(','))

def _operations(directory):
    for filename1,filename2 in zip(glob(directory+"/*/AllReads.log"), glob(directory+"/*/AllWrites.log")):
        with open(filename1,'r') as f1:
            with open(filename2,'r') as f2:
                operations1 = sum(int(get_last_number(line)) for line in f1 if "=" in line)
                operations2 = sum(int(get_last_number(line)) for line in f2 if "=" in line)
                if operations1 > 0 or operations2 > 0:
                    yield (operations1,operations2)

def _top(directory,num):
    for filename in glob(directory+"/*/AllReads.log"):
        with open(filename,'r') as f:
            gen = ((line,int(get_last_number(line))) for line in f if "=" in line)
            yield [x[0].split('=')[0].strip() for x in sorted(gen, key=lambda x: x[1])[:num]]

def _check_items(directory):
    for filename in glob(directory+"/*/LargeReplSet.log"):
        with open(filename,'r') as f:
            line = f.read()
            yield len(line.split(","))

def _settings(directory):
    with open(directory + "/settings.out", 'r') as f:
        settings = eval(f.read())

    keys = filter(lambda x: any(name in x for name in names), settings)
    relevant_settings = "\t".join(
        "%s:%s" % (x, settings[x]) for x in keys)
    return relevant_settings

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return float(val[-1])

if __name__ == '__main__':
    arguments = docopt(__doc__, version='1.0')
    if arguments["--operations"]:
        operations()
    elif arguments["--top"]:
        top()
    elif arguments["--check_items"]:
        check_items()
    elif arguments['--large_repl']:
        large_repl()
    else:
        throughput()
