#!/user/bin/env python
__author__ = "Peter Rose"
__version__ = "0.2.0"

from pyspark.sql import DataFrame
from pyspark.sql.functions import explode
from pyspark.sql.types import ArrayType


# /**
#  * Removes spaces from column names to ensure compatibility with parquet
#  * files.
#  *
#  * @param original
#  *            dataset
#  * @return dataset with columns renamed
#  */
#


def remove_spaces_from_column_names(dataset: DataFrame):
    for name in dataset.columns:
        new_name = name.replace(" ", "")
        original = dataset.withColumnRenamed(name, new_name)

        return original


def flatten_dataset(dataset: DataFrame):
    tmp = dataset
    for field in tmp.schema.fields:
        if isinstance(field.dataType, ArrayType):
            print(field.name, field.dataType)
            tmp = tmp.withColumn(field.name, explode(tmp.field.name))

    return tmp
