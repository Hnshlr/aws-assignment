# PRINTER:
def outprint(stdout, stderr=None):
    if stdout.decode('utf-8') != '':
        print(stdout.decode('utf-8')[:-1])
    if stderr != None:
        if stderr.decode('utf-8') != '':
            print(stderr.decode('utf-8')[:-1])