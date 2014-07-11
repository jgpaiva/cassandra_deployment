#!/usr/bin/env python
from glob import glob
from docopt import docopt
import csv

from environment import numeric_const_pattern

names = ['replication','threads','processing']
names_replace = {'large_replication_degree':'large_repl',
                 'max_items_for_large_replication_degree':'max_items',
                 'replication_factor':'repl',
                 'readproportion':'read',
                 'updateproportion':'update',
                 'sleep_time':'pi',
                 'threads':'t',
                 'recordcount':'items',
                 'skew':'s',
                 'ignore':'i',
                 'ignore_non_local':'z',
                 'processing_nodes':'nodes'}

options = {}
def register_funct(funct):
    options[funct.__name__] = funct
    return funct

def iterdirs(funct):
    def retval():
        for directory in dirs:
            try:
                funct(directory)
            except IOError:
                print "Could not read files for %s" % directory
    retval.__name__=funct.__name__
    return retval

def printval(funct):
    def retval(directory):
        relevant_settings = _settings(directory)
        res = funct(directory)
        print("%s %s %s" % (relevant_settings,res,directory))
    retval.__name__=funct.__name__
    return retval

@register_funct
@iterdirs
@printval
def operations(directory):
    val = list(_operations(directory))
    val = [[int(i[j]) for i in val] for j in range(2)]
    return [(sum(i), i) for i in val]

@register_funct
@iterdirs
@printval
def runtime(directory):
    val = list(_runtime(directory))
    return int(sum(val)/len(val)),val

@register_funct
@iterdirs
@printval
def operations_issued(directory):
    val = list(_operations_issued(directory))
    val = [[int(i[j]) for i in val] for j in range(2)]
    return [(sum(i), i) for i in val]

@register_funct
@iterdirs
@printval
def large_repl(directory):
    return list(_large_repl(directory))

@register_funct
@iterdirs
@printval
def top(directory):
    gen = _top(directory,5)
    top_freqs = (",".join(str(x[1]) for x in y) for y in gen if y)
    return "; ".join(top_freqs)

@register_funct
@iterdirs
@printval
def top_items(directory):
    gen = _top(directory,5)
    top_freqs = (",".join(str(x[0]) for x in y) for y in gen if y)
    return "; ".join(top_freqs)

@register_funct
@iterdirs
@printval
def latency_list(directory):
    labels = 'u: u95: r: r95:'.split()
    val = list(_latency(directory))
    retval = list()
    for j in range(4):
        retval.append(labels[j] + ",".join([sizeof_fmt(i[j]) for i in val]))
    return "; ".join(retval)

@register_funct
@iterdirs
@printval
def latency(directory):
    val = list(_latency(directory))
    retval = list()
    for j in range(4):
        try:
            retval.append(int(sum(i[j] for i in val)/len(val)))
        except TypeError:
            retval.append(None)
    return retval


@register_funct
@iterdirs
@printval
def check_items(directory):
    return list(_check_items(directory))

@register_funct
@iterdirs
@printval
def idle_cpu(directory):
    return _min_max_csv(directory,2,False)

@register_funct
@iterdirs
@printval
def idle_cpu_list(directory):
    retval = []
    for glob_pattern in [directory+"/*/dstat_server.csv",directory+"/*/dstat_ycsb.csv"]:
        retval.append(",".join(map(str,_average_csv_column(glob_pattern,2))))
    return ";".join(retval)

@register_funct
@iterdirs
@printval
def net_list(directory):
    in_ = []
    for glob_pattern in [directory+"/*/dstat_server.csv",directory+"/*/dstat_ycsb.csv"]:
        in_.append(",".join(map(sizeof_fmt,_average_csv_column(glob_pattern,8))))
    out = []
    for glob_pattern in [directory+"/*/dstat_server.csv",directory+"/*/dstat_ycsb.csv"]:
        out.append(",".join(map(sizeof_fmt,_average_csv_column(glob_pattern,9))))
    return ";".join(in_) + "\t" + ";".join(out)

@register_funct
@iterdirs
@printval
def disk(directory):
    read = _min_max_csv(directory,6)
    write = _min_max_csv(directory,7)
    return "r:" + str(read) +" w:" + str(write)

@register_funct
@iterdirs
@printval
def net(directory):
    in_ = _min_max_csv(directory,8)
    out = _min_max_csv(directory,9)
    return "in:" + str(in_) +" out:" + str(out)

def _min_max_csv(directory,index,byte_format=True):
    retval = []
    for glob_pattern in [directory+"/*/dstat_server.csv",directory+"/*/dstat_ycsb.csv"]:
        val = list(_average_csv_column(glob_pattern,index))
        avg = sum(val)/len(val)

        if byte_format:
            retval.append(",".join(
                map(sizeof_fmt,[avg,max(val),min(val)])))
        else:
            retval.append(",".join(
                map(str,[avg,max(val),min(val)])))
    return ";".join(retval)

def _dstat_filter(reader):
    CSW_COLUMN = 13
    for index, row in enumerate(reader):
        if index < 3:
            continue
        all_items = []
        for item in row:
            try:
                all_items.append(float(item))
            except ValueError:
                pass
        if index < 20 or all_items[CSW_COLUMN] >= 500:
            yield all_items
        else:
            return

def _average_csv_column(directory,index):
    for reader in _read_csvs(directory):
        all_lines = [float(row[index]) for row in _dstat_filter(reader)]
        #all_lines = [float(row[index]) for row in reader]
        yield int(sum(all_lines)/len(all_lines))

def _read_csvs(directory):
    for file_name in glob(directory):
        with open(file_name,'r') as f:
            lines = (i for i in f if not i.startswith('"') and len(i) > 1)
            reader = csv.reader(lines)
            yield reader

@register_funct
@iterdirs
def throughput(directory):
    try:
        relevant_settings = _settings(directory)

        throughputs = list(int(i) for i in _throughputs(directory))
        try:
            throughput = sum(throughputs)
        except:
            throughput = "N/A"
        throughputs = "["+", ".join(map(sizeof_fmt,throughputs))+"]"

        print("%s %s %s %s" % (relevant_settings, sizeof_fmt(throughput),throughputs,directory))
    except:
        print("error for directory %s" % (directory))

def _throughputs(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            thrp_strs = filter(lambda x: x.startswith("[OV") and "Throughput" in x,f)
            yield get_last_number(thrp_strs[0])

def _latency(directory):
    for run_out in glob(directory+"/*/run.out"):
        def get_lat(str1, str2):
            try:
                string = filter(lambda x: x.startswith(str1) and str2 in x,f)
                return  get_last_number(string[0])
            except IndexError:
                return None

        with open(run_out,'r') as file_iter:
            f = file_iter.readlines()

        avg_update = get_lat('[UPDATE','AverageLatency')
        ninety_update = get_lat('[UPDATE','95thPercentileLatency')
        avg_read = get_lat('[READ','AverageLatency')
        ninety_read = get_lat('[READ','95thPercentileLatency')

        yield (avg_update,ninety_update,avg_read,ninety_read)

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

def _operations_issued(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            whole_file = f.readlines()
        updates = int(get_last_number([x for x in whole_file if x.startswith('[UPDATE], Operations')][0]))
        reads   = int(get_last_number([x for x in whole_file if x.startswith('[READ], Operations')][0]))
        yield (updates,reads)

def _runtime(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            whole_file = f.readlines()
        runtime = int(get_last_number([x for x in whole_file if x.startswith('[OVERALL], RunTime(ms)')][0]))
        yield runtime

def _top(directory,num):
    for filename in glob(directory+"/*/AllReads.log"):
        with open(filename,'r') as f:
            gen = ((line,int(get_last_number(line))) for line in f if "=" in line)
            top_sorted = sorted(gen, key=lambda x: x[1],reverse=True)[:num]
            yield [(x[0].split('=')[0].strip(),x[1]) for x in top_sorted]

def _check_items(directory):
    for filename in glob(directory+"/*/LargeReplSet.log"):
        with open(filename,'r') as f:
            line = f.read()
            yield len(line.split(","))

def _settings(directory):
    with open(directory + "/settings.out", 'r') as f:
        settings = eval(f.read())

    keys = filter(lambda x: any(name in x for name in names), settings)
    relevant_settings = " ".join(
        "%s:%s" % (names_replace[x],
                   str(settings[x]).ljust(2) if x is not 'readproportion'
                   else str(settings[x]).ljust(5))
        for x in keys)
    return relevant_settings

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return float(val[-1])

def sizeof_fmt(num):
    if num is None:
        return str(num)
    for x in ['','K','M','G']:
        if num < 1000.0 and num > -1000.0:
            return "%3.1f%s" % (num, x)
        num /= 1000.0
    return "%3.1f%s" % (num, 'T')

if __name__ == '__main__':
    usage = ("""Get statistics

Usage:
  stats.py [help|""" + "|".join(options.keys()) + """] [<prefix>]
""")

    arguments = docopt(usage, version='1.0')
    prefix = arguments['<prefix>']
    if prefix:
        dirs = glob(prefix+"/results.*")
    else:
        dirs = glob("results.*")

    for i in options:
        if arguments[i]:
            options[i]()
            break
    else:
        print(usage)
