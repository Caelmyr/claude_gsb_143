#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run.py — 顶层启动脚本：python3 run.py [--datanodes 3] [--reset]"""
import sys

from backend.main import main

if __name__ == "__main__":
    sys.exit(main())
