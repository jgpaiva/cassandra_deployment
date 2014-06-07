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

        try:
            throughput = []
            for run_out in glob(directory+"/*/run.out"):
                with open(run_out,'r') as f:
                    run_out = f.readlines()
                thrp_strs = filter(lambda x: "Throughput" in x,run_out)
                throughput.append(thrp_strs[0])
            throughput = sum([get_last_number(t) for t in throughput])
        except:
            throughput = "N/A"

        print "%s %s %s" % (relevant_settings,throughput,directory)

def get_last_number(string):
    val = numeric_const_pattern.findall(string)
    return float(val[-1])

main()
