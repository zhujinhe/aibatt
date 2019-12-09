import time
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph
from rq import Queue, Connection
from rq.job import Job
from redis import Redis


class WorkFlow(object):

    def __init__(self, redis_conn=Redis('localhost', 6379)):
        """初始化workflow"""

        self.redis_conn = redis_conn
        self.graph = nx.DiGraph()
        self.tasks = {}
        self.tasks_in_queue = None
        self.tasks_started = []

    def load_from_dict(self, d):
        """从字典加载依赖关系dag_adjacency_list"""
        self.graph = nx.DiGraph()

        for node in d.keys():
            nodes = d[node]
            if len(nodes) == 0:
                self.graph.add_node(int(node))
                continue
            self.graph.add_edges_from([(int(node), n) for n in nodes])
        return self.graph

    def add_task(self, task_name, func, *args, **kwargs):
        print("add_task", task_name, func, args, kwargs)

        if task_name in self.tasks:
            raise ValueError('Already exit, uniq task_name needed.')
        self.graph.add_node(task_name)
        self.tasks[task_name] = {"func": func, "args": args, "kwargs": kwargs}

    def add_relation(self, u, v):
        """依赖关系, u -> v"""
        print("{} depend on {}".format(v, u))
        self.graph.add_edge(u, v)
        if not is_directed_acyclic_graph(self.graph):
            print("NOT A DAG, ignore")
            self.graph.remove_edge(u, v)

    def run(self, run_interval=1):
        """按照图的依赖关系来执行所有动作"""
        while True:
            print("tasks_in_queue", self.tasks_in_queue)
            if self.tasks_in_queue is None:
                self.tasks_in_queue = set()
                self.tasks_in_queue.update(self.find_entry_point())
            else:
                for task_id in self.tasks_in_queue.copy():
                    if task_id not in self.tasks_started and self.is_task_ready(task_id):
                        self.run_task(task_id)
                        for next_task_id in self.graph.successors(task_id):
                            self.tasks_in_queue.add(next_task_id)
                if len(self.tasks_in_queue) == 0:
                    return
                time.sleep(run_interval)

    def is_task_ready(self, task_id):
        """检查当前任务前置任务是否都是finished"""
        dep_tasks = set(self.graph.predecessors(task_id))
        for dep_task in dep_tasks:
            with Connection(self.redis_conn):
                if not Job.fetch(dep_task).is_finished:
                    return False
        return True

    def run_task(self, task_id):
        """向worker发送任务"""

        print("try run task", task_id)
        if task_id in self.tasks_started:
            return

        try:
            task = self.tasks.get(task_id, {})
            q = Queue(connection=self.redis_conn)
            job = q.enqueue(task['func'], *task['args'], **task['kwargs'], job_id=task_id)
            job.save()
        except:
            print("Exception")
            pass
        else:
            self.tasks_in_queue.discard(task_id)
            self.tasks_started.append(task_id)

    def find_entry_point(self):
        """找到task的起点"""
        entry_point = set()
        for node in self.graph.nodes:
            if len(set(self.graph.predecessors(node))) == 0:
                entry_point.add(node)
        return entry_point
