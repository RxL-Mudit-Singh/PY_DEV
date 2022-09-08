from time import sleep
from progress.bar import Bar

# with Bar('Cases Processed...') as bar:
#     for i in range(100):
#         sleep(0.02)
#         bar.next()
        
        
        
# with Bar('Loading', fill='@', suffix='%(percent).1f%% - %(eta)ds') as bar:
#     for i in range(100):
#         sleep(0.02)
#         bar.next()


from alive_progress import alive_bar
from time import sleep

# with alive_bar(100) as bar:   # default setting
#     for i in range(100):
#         sleep(0.03)
#         bar()                        # call after consuming one item

# using bubble bar and notes spinner
with alive_bar(100, bar = 'blocks', spinner = 'dots_waves2') as bar:
    for i in range(100):
        sleep(0.02)
        bar()  
