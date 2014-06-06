from glob import glob

from environment import numeric_const_pattern

names = ['replication']

dirs = glob("results.*")

def main():
    for directory in dirs:
        with open(directory + "/settings.out", 'r') as f:
            settings = eval(f.read())

        keys = filter(lambda x: any(name in x for name in names), settings)
        relevant_settings = ", ".join(
            map(lambda x: "%s:%s" % (x, settings[x]), keys))

        throughput = []
        for run_out in glob(directory+"/*/run.out"):
            with open(run_out,'r') as f:
                run_out = f.readlines()
            throughput.append(filter(lambda x: "Throughput" in x,run_out)[0])
        throughput = [get_last_number(t) for t in throughput]

        print "%s %s" % (relevant_settings,throughput)

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return val[-1]

main()
