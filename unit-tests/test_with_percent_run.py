# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *


class TestPercentRunFixture(NutterFixture):
  def assertion_upper_columns_percent_run(self):
    cols = ["col1", "col2", "col3"]
    df = spark.createDataFrame([("abc", "cef", 1)], cols)
    expected_df = spark.createDataFrame([("abc", "cef", 1)], cols)
    assert_df_equality(upper_df, expected_df)

# COMMAND ----------

result = TestPercentRunFixture().execute_tests()
result.exit(dbutils)

# COMMAND ----------


