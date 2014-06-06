from glob import glob

names = ['replication']

dirs = glob("results.*")
settings = glob("results.*/settings.out")

def read_file(path):
    with open(path, 'r') as f:
        return f.read()

settings = [eval(read_file(f)) for f in settings]

settings_print = []
for i in settings:
    keys = filter(lambda x: any(name in x for name in names), i)
    settings_print.append(
        ", ".join(map(lambda x:"%s:%s" % (x, i[x]),keys)))

throughputs = []
for i in dirs:
    for j in glob(i+"/172*/run.out"):
        f = read_file(j)
        a = filter(lambda x: "Throughput" in x,f.split('\n'))[0]
        throughputs.append(a)

for i,j in zip(settings_print, throughputs):
    print "%s %s" % (i,j)
