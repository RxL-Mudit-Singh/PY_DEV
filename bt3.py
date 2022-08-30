import multiprocessing
import time
 
def cube(x):
    return x**3

def hi():
    print("HI")
    print("""
    |
    |
    |
    |
    |
    """)
 
if __name__ == "__main__":
    pool = multiprocessing.Pool()
    start_time = time.perf_counter()
    processes = [pool.apply_async(hi), range(1,20)]
    result = [p.get() for p in processes]
    finish_time = time.perf_counter()
    print(f"Program finished in {finish_time-start_time} seconds")
    print(result)