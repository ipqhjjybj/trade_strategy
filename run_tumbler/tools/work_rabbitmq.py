# coding=utf-8
import sys

from tumbler.function.tools import queue_delete

if __name__ == "__main__":
    if len(sys.argv) > 0:
        data = queue_delete(sys.argv[1])
        print(data)
