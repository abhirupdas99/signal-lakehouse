# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Run the unit tests
# MAGIC The attribution tests fail until you implement `last_touch_attribution` in `transforms.py`.
# MAGIC That's expected: the tests are your spec.

# COMMAND ----------

# MAGIC %pip install pytest -q

# COMMAND ----------

import os
import sys

import pytest

dbutils.widgets.text("repo_path", "")
repo = dbutils.widgets.get("repo_path") or os.getcwd()
os.chdir(repo)
sys.dont_write_bytecode = True   # workspace files don't like .pyc files

exit_code = pytest.main(["-q", "-p", "no:cacheprovider", "tests"])
assert exit_code == 0, "Some tests failed: read the output above."
