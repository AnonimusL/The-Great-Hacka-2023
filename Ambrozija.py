# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - Data transformation

# COMMAND ----------

import pandas as pd
from pyspark.sql import Row, Column
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC Podaci o biljkama i njihova jacina polena

# COMMAND ----------

file_location_plants = "dbfs:/FileStore/tables/pollen_type.csv"

#schema 
schema_def_plants = StructType([StructField('ID_VRSTE', IntegerType(), True),
                     StructField('IME_VRSTE_LAT', StringType(), True),
                     StructField('IME_VRSTE_SRB', StringType(), True),
                     StructField('GRUPA_BILJAKA', StringType(), True),
                     StructField('ALERGENI_POTENCIJAL', IntegerType(), True)
])

# COMMAND ----------

plants = spark.read.csv(file_location_plants, encoding="UTF-8", header=True, schema=schema_def_plants) 

# COMMAND ----------

plants = plants.withColumn("IME_VRSTE_LAT", upper(col("IME_VRSTE_LAT")))


display(plants)

# COMMAND ----------

file_location_train = "dbfs:/FileStore/tables/pollen_train.csv"

#schema 
schema_def_train = StructType([
                     StructField('ID', IntegerType(), True),
                     StructField('LOCATION', StringType(), True),
                     StructField('DATE', StringType(), True),
                     StructField('ACER', IntegerType(), True),
                     StructField('ALNUS', IntegerType(), True),
                     StructField('AMBROSIA', IntegerType(), True),
                     StructField('ARTEMISIA', IntegerType(), True),
                     StructField('BETULA', IntegerType(), True),
                     StructField('CANNABACEAE', IntegerType(), True),
                     StructField('CARPINUS', IntegerType(), True),
                     StructField('CELTIS', IntegerType(), True),
                     StructField('CHENOP/AMAR.', IntegerType(), True),
                     StructField('CORYLUS', IntegerType(), True),
                     StructField('CUPRESS/TAXA.', IntegerType(), True),
                     StructField('FAGUS', IntegerType(), True),
                     StructField('FRAXINUS', IntegerType(), True),
                     StructField('JUGLANS', IntegerType(), True),
                     StructField('MORACEAE', IntegerType(), True),
                     StructField('PINACEAE', IntegerType(), True),
                     StructField('PLANTAGO', IntegerType(), True),
                     StructField('PLATANUS', IntegerType(), True),
                     StructField('POACEAE', IntegerType(), True),
                     StructField('POPULUS', IntegerType(), True),
                     StructField('QUERCUS', IntegerType(), True),
                     StructField('RUMEX', IntegerType(), True),
                     StructField('SALIX', IntegerType(), True),
                     StructField('TILIA', IntegerType(), True),
                     StructField('ULMACEAE', IntegerType(), True),
                     StructField('URTICACEAE', IntegerType(), True)
])

# COMMAND ----------

file_location_test = "dbfs:/FileStore/tables/pollen_test.csv"

#schema 
schema_def_test = StructType([
                     StructField('ID', IntegerType(), True),
                     StructField('LOCATION', StringType(), True),
                     StructField('DATE', StringType(), True),
                     StructField('ACER', IntegerType(), True),
                     StructField('ALNUS', IntegerType(), True),
                     StructField('AMBROSIA', IntegerType(), True),
                     StructField('ARTEMISIA', IntegerType(), True),
                     StructField('BETULA', IntegerType(), True),
                     StructField('CANNABACEAE', IntegerType(), True),
                     StructField('CARPINUS', IntegerType(), True),
                     StructField('CELTIS', IntegerType(), True),
                     StructField('CHENOP/AMAR.', IntegerType(), True),
                     StructField('CORYLUS', IntegerType(), True),
                     StructField('CUPRESS/TAXA.', IntegerType(), True),
                     StructField('FAGUS', IntegerType(), True),
                     StructField('FRAXINUS', IntegerType(), True),
                     StructField('JUGLANS', IntegerType(), True),
                     StructField('MORACEAE', IntegerType(), True),
                     StructField('PINACEAE', IntegerType(), True),
                     StructField('PLANTAGO', IntegerType(), True),
                     StructField('PLATANUS', IntegerType(), True),
                     StructField('POACEAE', IntegerType(), True),
                     StructField('POPULUS', IntegerType(), True),
                     StructField('QUERCUS', IntegerType(), True),
                     StructField('RUMEX', IntegerType(), True),
                     StructField('SALIX', IntegerType(), True),
                     StructField('TILIA', IntegerType(), True),
                     StructField('ULMACEAE', IntegerType(), True),
                     StructField('URTICACEAE', IntegerType(), True),
                     StructField('BATCH_ID', IntegerType(), True)
])

# COMMAND ----------

test = spark.read.csv(file_location_test, encoding="UTF-8", header=True, schema=schema_def_test) 

# COMMAND ----------

display(test)

# COMMAND ----------

train = spark.read.csv(file_location_train, encoding="UTF-8", header=True, schema=schema_def_train) 

# COMMAND ----------

display(train)

# COMMAND ----------

# only potentional plants

potentional_plants = plants.where(col("ALERGENI_POTENCIJAL") == 2)

display(potentional_plants)


# COMMAND ----------

file_location_weather = "dbfs:/FileStore/tables/weather_data.csv"

#schema 
schema_def_weather = StructType([StructField('ID', IntegerType(), True),
                     StructField('LOCATION', StringType(), True),
                     StructField('DATE', StringType(), True),
                     StructField('tavg', DoubleType(), True),
                     StructField('tmin', DoubleType(), True),
                     StructField('tmax', DoubleType(), True),
                     StructField('prcp', DoubleType(), True),
                     StructField('snow', DoubleType(), True),
                     StructField('wdir', DoubleType(), True),
                     StructField('wspd', DoubleType(), True),
                     StructField('wpgt', DoubleType(), True),
                     StructField('pres', DoubleType(), True),
                     StructField('tsun', DoubleType(), True)
])

# COMMAND ----------

weather = spark.read.csv(file_location_weather, encoding="UTF-8", header=True, schema=schema_def_weather) 
weather = weather.sort(weather.DATE, weather.LOCATION)



display(weather)
display(weather.count())

# COMMAND ----------

class TemperatureData:
    def __init__(self, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun):
        self.tavg = tavg
        self.tmin = tmin
        self.tmax = tmax
        self.prcp = prcp
        self.snow = snow
        self.wdir = wdir
        self.wspd = wspd
        self.wpgt = wpgt
        self.pres = pres
        self.tsun = tsun

# COMMAND ----------

# Collect distinct values from the DataFrame
cities_list = [row.LOCATION for row in weather.select(weather.LOCATION).distinct().collect()]
dates_list = [row.DATE for row in weather.select(weather.DATE).distinct().collect()]

data_dict = {}
data_list = []
for row in weather.rdd.collect():
    data_list = []
    city = row.LOCATION
    date = row.DATE
    key = f"{city}_{date}"
    """
    temperature_data = {
        "tavg": row.tavg,
        "tmin": row.tmin,
        "tmax": row.tmax,
        "wdir": row.wdir,
        "wspd": row.wspd,
        "pres": row.pres
    }
    """
    data_list.append(row.tavg);
    data_list.append(row.tmin);
    data_list.append(row.tmax);
    data_list.append(row.wdir);
    data_list.append(row.wspd);
    data_list.append(row.wpgt);
    data_list.append(row.pres);

    data_dict[key] = data_list

sorted_data = dict(sorted(data_dict.items()))

for key, data in sorted_data.items():    
    print(f"{key}: {data}")


# COMMAND ----------

from datetime import datetime

# COMMAND ----------

min_date = train.select(min(train.DATE))
display(min_date)

max_date = train.select(max(train.DATE))
display(max_date)

# COMMAND ----------

data_dict = sorted_data
for key, data in data_dict.items():
    year = int(key.split("_")[1].split('-')[0])
    date_obj = datetime.strptime(key.split("_")[1], "%Y-%m-%d")
    month = date_obj.month
    day = date_obj.day
    rest = key.split("_")[1].split('-',1)[1]
    for i in range(0, len(data)):
        if not data[i]:
            key_to_check1 = 0.0
            key_to_check2 = 0.0
            key_to_check3 = 0.0
            key_to_check4 = 0.0
            sum = 0.0
            cnt = 0

            for j in range(2016, 2022):
                if data_dict.get(city + "_" + str(j) + "-" + rest) is not None:
                    key_to_check1 = data_dict[city + "_" + str(j) + "-" + rest][i]
                    if key_to_check1 is not None:
                        sum += key_to_check1
                        cnt += 1
                if data_dict.get(city + "_" + str(j) + "-" + str(month) + "-" + str(day-1)) is not None:
                    key_to_check2 = data_dict[city + "_" + str(j) + "-" + str(month) + "-" + str(day-1)][i]
                    if key_to_check2 is not None:
                        sum += key_to_check2
                        cnt += 1
                if data_dict.get(city + "_" + str(j) + "-" + str(month) + "-" + str(day+1)) is not None:
                    key_to_check3 = data_dict[city + "_" + str(j) + "-" + str(month) + "-" + str(day+1)][i]
                    if key_to_check3 is not None:
                        sum += key_to_check3
                        cnt += 1

            if cnt > 0:
                data[i] = sum/cnt
            else:
                print(key)

for key, data in data_dict.items():
    for i in range(0, len(data)):
        if not data[i]: 
            city = key.split("_")[0]
            date = key.split("_")[1]
            if "КРАГУЈЕВАЦ" in city and data_dict["БЕОГРАД - НОВИ БЕОГРАД_"+date][i] and data_dict["НИШ_"+date][i]:
                data[i] = (data_dict["БЕОГРАД - НОВИ БЕОГРАД_"+date][i]+data_dict["НИШ_"+date][i])/2
            if "ПОЖАРЕВАЦ" in city and data_dict["НИШ_"+date][i]:
                data[i] = data_dict["НИШ_"+date][i]
   
for key, data in data_dict.items():
    for i in range(0, len(data)):
        if not data[i]: 
            city = key.split("_")[0]
            date = key.split("_")[1]
            if "КРАЉЕВО" in city and data_dict["КРАГУЈЕВАЦ_"+date][i] and data_dict["НИШ_"+date][i]:
                data[i] = (data_dict["КРАГУЈЕВАЦ_"+date][i]+data_dict["НИШ_"+date][i])/2

            

# COMMAND ----------

for key, data in data_dict.items():
    year = int(key.split("_")[1].split('-')[0])
    rest = key.split("_")[1].split('-',1)[1]
    for i in range(0, 3):
        if not data[i]:
            data[i] = 0.0

# COMMAND ----------

print(len(data_dict))
dict_to_list = []
inner_list = []

for key, data in data_dict.items():
    inner_list = []
    city = key.split("_")[0]
    date = key.split("_")[1]
    rest = key.split("_")[1].split('-',1)[1]

    inner_list.append(city)
    inner_list.append(date)
    for i in range(0, len(data)):
        if data[i] is not None:
            inner_list.append(data[i])
        else:
            print("Greska")
            print(key)
            print(data[i])
    
    dict_to_list.append(inner_list)

fixed_weather = pd.DataFrame(dict_to_list, columns=['LOCATION', 'DATE', 'tavg', 'tmin', 'tmax', 'wdir', 'wspd', 'wpgt' , 'pres'])


# COMMAND ----------

display(fixed_weather)

# COMMAND ----------

display(train)
display(fixed_weather)

# COMMAND ----------

weather = spark.createDataFrame(fixed_weather).cache()
display(weather.count())
weather.where((weather.LOCATION == 'СУБОТИЦА') & (weather.DATE == '2021-03-28')).show()

my_weather = weather.select(weather.LOCATION, weather.DATE, weather.tavg)
display(my_weather)

weather = weather.withColumnRenamed("DATE", "DATE 1")
weather = weather.withColumnRenamed("LOCATION", "LOCATION 1")

# COMMAND ----------

display(weather)

# COMMAND ----------

print(cities_list)

# COMMAND ----------

from pyspark.sql.functions import to_date, month, dayofmonth, year

# COMMAND ----------

condition1 = (train["DATE"] == weather["DATE 1"])
condition2 = (train["LOCATION"] == weather["LOCATION 1"])

for_matrix = train.join(weather, (condition1) & (condition2), "inner")
display(for_matrix)
# Show the joined DataFrame
for_matrix = for_matrix.withColumn("DATE", to_date(for_matrix["DATE"]))
for_matrix = for_matrix.withColumn("Month", month(for_matrix["DATE"]))
for_matrix = for_matrix.withColumn("Day", dayofmonth(for_matrix["DATE"]))
for_matrix = for_matrix.withColumn("Year", year(for_matrix["DATE"]))


for_matrix = for_matrix.withColumn("City", when(for_matrix["LOCATION"] == "БЕОГРАД - НОВИ БЕОГРАД", 1)
    .when(for_matrix["LOCATION"] == "ВРШАЦ", 2)
    .when(for_matrix["LOCATION"] == "КРАГУЈЕВАЦ", 3)
    .when(for_matrix["LOCATION"] == "СУБОТИЦА", 4)
    .when(for_matrix["LOCATION"] == "КРАЉЕВО", 5)
    .when(for_matrix["LOCATION"] == "НИШ", 6)
    .when(for_matrix["LOCATION"] == "ПОЖАРЕВАЦ", 7)
    .otherwise(0)  # You can use 0 for locations not found in the mapping
)



display(for_matrix)
display(for_matrix.count())

# COMMAND ----------

# Correlation matrix

from pyspark.mllib.stat import Statistics
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import seaborn as sb
from scipy.stats import pearsonr
from decimal import Decimal
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

display(potentional_plants)

# COMMAND ----------

try_df = for_matrix

columns_to_remove = ["LOCATION", "DATE", "LOCATION 1", "DATE 1"]
corr_analysis_df = try_df.drop(*columns_to_remove)
corr_analysis_df = corr_analysis_df.withColumnRenamed("CHENOP/AMAR.", "CHENOP")
corr_analysis_df = corr_analysis_df.withColumnRenamed("CUPRESS/TAXA.", "CUPRESS")
vector_col = "col-features"
assembler = VectorAssembler(inputCols=corr_analysis_df.columns, outputCol=vector_col)
df_vector = assembler.transform(corr_analysis_df).select(vector_col)
matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
corr_matrix = matrix.toArray().tolist()

colums = corr_analysis_df.columns #["Day", "Month", "Year", "City", "AMBROSIA", "ALNUS", "BETULA", "POACEAE", "URTICACEAE", "tavg", "tmin", "tmax", "wdir", "wspd", "wpgt", "pres"]
# df_corr = spark.createDataFrame(corr_matrix, colums)
corr_matrix_df = pd.DataFrame(data=corr_matrix, columns = colums, index=colums) 
plt.figure(figsize=(24,24))  
sb.heatmap(corr_matrix_df, 
            xticklabels=corr_matrix_df.columns.values,
            yticklabels=corr_matrix_df.columns.values,  cmap="Greens", annot=True)

# COMMAND ----------

my = for_matrix
columns_to_remove = ["LOCATION 1", "DATE 1"]
my = my.drop(*columns_to_remove)
my = my.withColumnRenamed("CHENOP/AMAR.", "CHENOP")
my = my.withColumnRenamed("CUPRESS/TAXA.", "CUPRESS")

# COMMAND ----------

ambrosia = my.select(my.DATE,for_matrix.AMBROSIA, my.ARTEMISIA, my.CHENOP, my.CANNABACEAE, my.Day, my.Month, my.Year, my.tavg, my.City)
display(ambrosia.count())
#ambrosia.write.mode("overwrite").csv(path='dbfs:/FileStore/tables/train_ambrosia.csv', header=True)

# COMMAND ----------

# my_weather = weather.select(weather.LOCATION, weather.DATE, weather.tavg)
display(my_weather)

window_spec = Window.orderBy(F.monotonically_increasing_id())
df_with_row_number = my_weather.withColumn("row_number", F.row_number().over(window_spec))

display(df_with_row_number.count())
# Set the batch size
batch_size = 10000
offset = 0
read = 0
# Create a loop to paginate through the DataFrame
while True:
    # Display the next batch of data
    batch_df = df_with_row_number.filter((F.col("row_number") >= offset) & (F.col("row_number") < offset + batch_size))
    display(batch_df.drop("row_number"))
    
    # Increment the offset for the next batch
    offset += batch_size
    read += batch_df.count()
    # Check if there are more rows
    if read == df_with_row_number.count():
        break
