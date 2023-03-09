-- Databricks notebook source
-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import concat_ws,col
-- MAGIC from pyspark.sql.types import StructType,StructField, StringType
-- MAGIC  
-- MAGIC data = [("A1","B1","C1","D1"),
-- MAGIC          ("A2","B2","C2","D2"),
-- MAGIC          ("A3","B3","C3","D3"),
-- MAGIC          ("A4","B4","C3","D4")
-- MAGIC        ]
-- MAGIC  
-- MAGIC schema = StructType([ \
-- MAGIC     StructField("A",StringType(),True), \
-- MAGIC     StructField("B",StringType(),True), \
-- MAGIC     StructField("C",StringType(),True),\
-- MAGIC    StructField("D",StringType(),True)
-- MAGIC   ])
-- MAGIC  
-- MAGIC df = spark.createDataFrame(data=data,schema=schema)
-- MAGIC df.printSchema()
-- MAGIC df.show()
-- MAGIC  
-- MAGIC df.select(concat_ws('_',df.A,df.B,df.C).alias("ABC"),"D").show()

-- COMMAND ----------

with p_tag as (
select 
	p.id,
	string_agg(concat('"', t.name, '"'), ', ') as tag_names,
	tt.name as tag_type
from 
	person p
	inner join person_tag pt 
		on pt.person_id = p.id
	inner join tag t 
		on pt.tag_id = t.id 
	inner join tag_type tt 
		on t.tag_type_id = tt.id
group by
	p.id, p.profile_id, tt.name
)

select
	id,
	concat('"', tag_type, '"', ': ', '[', tag_names, ']') as tags
from 
	p_tag
