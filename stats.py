from glob import glob

from environment import numeric_const_pattern

names = ['replication']

dirs = glob("results.*")

def main():
    for directory in dirs:
        try:
            with open(directory + "/settings.out", 'r') as f:
                settings = eval(f.read())

            keys = filter(lambda x: any(name in x for name in names), settings)
            relevant_settings = ", ".join(
                "%s:%s" % (x, settings[x]) for x in keys)

            try:
                throughput = sum(get_throughputs(directory))
            except:
                throughput = "N/A"

            try:
                print("%s\t%.0f %s" % (relevant_settings,throughput,directory))
            except TypeError:
                print("%s\t%s %s" % (relevant_settings,throughput,directory))
        except IOError:
            print "Could not read files for %s" % directory

def get_throughputs(directory):
    for run_out in glob(directory+"/*/run.out"):
        with open(run_out,'r') as f:
            thrp_strs = filter(lambda x: "Throughput" in x,f)
        yield get_last_number(thrp_strs[0])

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return float(val[-1])

if __name__ == '__main__':
    main()
