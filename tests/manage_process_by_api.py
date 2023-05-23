from fastapi import FastAPI
import typing as t
import uvicorn
import time
import multiprocessing as mp
import threading
import psutil

app = FastAPI()

RUNNING_PROCESS: t.Dict[int, mp.Process] = {}

try:
    mp.set_start_method("spawn", force=True)
except RuntimeError as e:
    raise e

""" ps -ef|grep python
gitpod      4927    4491  0 03:36 pts/2    00:00:00 /home/gitpod/.pyenv/versions/3.11.1/bin/python tests/manage_process_by_api.py
gitpod      5031    4927  0 03:36 pts/2    00:00:00 /home/gitpod/.pyenv/versions/3.11.1/bin/python -c from multiprocessing.resource_tracker import main;main(16)
gitpod      5032    4927  0 03:36 pts/2    00:00:00 /home/gitpod/.pyenv/versions/3.11.1/bin/python -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=17, pipe_handle=21) --multiprocessing-fork
gitpod      5496    4927  1 03:38 pts/2    00:00:00 /home/gitpod/.pyenv/versions/3.11.1/bin/python -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=17, pipe_handle=24) --multiprocessing-fork 
"""

def get_running_processes():
    res = {k: {"pid": v.pid, "alive": v.is_alive(), "name": v.name} for k, v in RUNNING_PROCESS.items()}

    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    res["children"] = [child.pid for child in children]
    return res

def run_simple_loop(name: str):
    while True:
        print(name, "do something")
        time.sleep(5)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/run/process/{id}")
async def run_process(id: str):
    p = mp.Process(name="process"+id, target=run_simple_loop, kwargs={"name": id})
    p.daemon = True
    p.start()
    RUNNING_PROCESS[str(p.pid)] = p
    return get_running_processes()

@app.get("/get/process")
async def get_processes():
    return get_running_processes()

@app.get("/stop/process/{id}")
async def run_process(id: str):
    process = RUNNING_PROCESS[str(id)]
    threading.Timer(2, process.terminate).start()
    # process.terminate()
    return get_running_processes()


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=5000)
    # can open multiple process per server => ok
    # when server is stop, all processes is killed => ok
    # 