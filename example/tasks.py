# -*- coding: utf8 -*-
import time
from datetime import datetime


def sleep1(name):
    print("sleep1", name, datetime.utcnow())
    time.sleep(1)
    return name


def sleep2(name):
    print("sleep2", name, datetime.utcnow())
    time.sleep(10)
    return name


def sleep3(name):
    print("sleep3", name, datetime.utcnow())
    time.sleep(2)
    return name
