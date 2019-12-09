# -*- coding: utf8 -*-

from alibatt.workflow import WorkFlow
from .tasks import sleep1, sleep2, sleep3

if __name__ == '__main__':
    wf1 = WorkFlow()
    wf1.add_task('job1', sleep1, "sl1")
    wf1.add_task('job2', sleep2, "sl2")
    wf1.add_task('job3', sleep3, "sl3")
    wf1.add_task('job4', sleep1, "sl4")

    wf1.add_relation('job1', 'job2')
    wf1.add_relation('job1', 'job3')
    wf1.add_relation('job2', 'job4')
    wf1.add_relation('job3', 'job4')

    print(wf1.graph)

    wf1.run()
