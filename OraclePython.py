
"""
;==========================================
; Title:  Data Validation with Apache Spark and Python
; Author: Harshal Vasant Dhake
; Date:   15-Aug-2019
;==========================================
"""
from __future__ import print_function
import generateSql
import math
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, udf, when, lit, concat, length
from pyspark import SparkContext


def build_schema(tableName):
    """Build and return a schema to use for the sample data."""

    statement = "(select column_name, case when data_type='VARCHAR2' then 'String'  when data_type='CHAR' then 'String'  when data_type='DATE' then 'String'  when data_type='NVARCHAR2' then 'String'  when data_type='NUMBER' then 'String'  else 'String' end AS data_type, nullable from dba_tab_columns where table_name ='" + tableName + "' order by column_id asc )"

    buildSchemaList = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()

    xList = buildSchemaList.collect()
    type_map = {'String': StringType(), 'Integer': IntegerType(), 'Date': DateType()}
    type_null_map = {'Y': True, 'N': False}
    cols = [StructField(x.COLUMN_NAME, type_map.get(x.DATA_TYPE, StringType()), type_null_map.get(x.NULLABLE,True)) for x in xList]
    schema = StructType(cols)
    
    return schema

def build_val_col_list(tableName):
    """Build and return a schema to use for the sample data."""
    statement = "( SELECT column_name, data_type, case when data_type='NUMBER' THEN NVL(DATA_PRECISION,38) + DATA_SCALE ELSE DATA_LENGTH END AS ORACLE_LENGTH FROM dba_tab_columns WHERE  table_name = '" + tableName + "' order by column_id asc )"
    buildColTypeList = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()

    xList = buildColTypeList.collect()

    return xList

def build_column_type_list(tableName):
    """Build and return a schema to use for the sample data."""

    statement = "(select column_name, case when data_type='VARCHAR2' then 'string'  when data_type='CHAR' then 'string'  when data_type='DATE' then 'date'  when data_type='NVARCHAR2' then 'string'  when data_type='NUMBER' and data_scale = 0 then 'long'  when data_type='NUMBER' and data_scale > 0 then 'decimal('|| data_precision ||','||data_scale ||')' else 'String' end AS data_type from dba_tab_columns where table_name ='" + tableName + "' order by column_id asc )"

    buildColTypeList = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()

    xList = buildColTypeList.collect()
    print(xList)
    return xList

def build_nullable_list(tableName):
    statement = "(select column_name from all_tab_columns where nullable = 'N' and table_name= '" + tableName + "' order by column_id asc)t"
    nullColList = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()
    
    stringsDS = nullColList.rdd.map(lambda row: "%s" % (row.COLUMN_NAME))

    return stringsDS

def checkIntData(columnData):
    status = "GOOD" 
    try:
        columnData.toInt
    except ValueError as ve:
        if columnData == null:
            print('Do nothing')
        elif columnData.length == 0:
            status = "ERROR"
        else:
            status = "BAD"
    return status

def build_pk_list():
    """Build and return a schema to use for the sample data."""

    statement = "(select a.column_name FROM all_cons_columns a JOIN all_constraints  c  ON (a.owner  = c.owner AND a.constraint_name   = c.constraint_name)   WHERE c.constraint_type='P' and a.table_name= '" + tableName  + "' order by a.position asc ) "
    PK = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()

    stringsDS = PK.rdd.map(lambda row: "%s" % (row.COLUMN_NAME))

    countList = 0
    for x in stringsDS.collect():
        if countList == 0:
            columnList = x
            countList = countList + 1
        else:
            columnList = columnList + ',' + x
            countList = countList +1
    return columnList

def build_uniq_idx_list(tableName):
    """Build and return a schema to use for the sample data."""
    statement = "(select a.index_name, listagg (a.column_name, ',')  WITHIN GROUP (ORDER BY a.column_position) COLUMN_NAMES FROM    dba_ind_columns a, dba_indexes b WHERE   a.table_name= '" + tableName  + "' AND a.table_name = b.table_name AND a.index_name= b.index_name AND b.uniqueness = 'UNIQUE' AND b.status = 'VALID' group by a.index_name)t"

    uindexes = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", statement) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()
    return uindexes

def check_tbl_exist(spark, tableName):
    query = "(select * from DBA_TABLES WHERE TABLE_NAME ='" + tableName + "')t"

    #Read data from Oracle Table
    tblCount = spark.read.format("jdbc") \
        .option("url","jdbc:oracle:thin:system/oracle@//0.0.0.0:1521/xe") \
        .option("dbtable", query) \
        .option("user","system") \
        .option("password","oracle") \
        .option("driver","oracle.jdbc.driver.OracleDriver") \
        .load()
    
    returnCount =  tblCount.count()
    if returnCount == 1:
        print('Table '+ tableName + ' exist in Database')
    else:
        print('Table Name '+ tableName + ' does not exist in database . Kindly check')
        exit()

def oracle_data_validation(spark, tableName,fileName):
    # $example on:programmatic_schema$
    sc = spark.sparkContext
    lines = spark.read.option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv(fileName)

    lines = sc.textFile(fileName)

    parts = lines.map(lambda l: l.split(","))

    fileParts = parts.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]))

    schemaDF = spark.createDataFrame(fileParts, schema=build_schema(tableName))

    colTypeList = build_column_type_list(tableName)

    schemaDF.printSchema()

    #schemaDF = schemaDF.collect()

    #schemaDF.show()

    colValidationTypeList = build_val_col_list(tableName)

    for i in colValidationTypeList:
        if (i.DATA_TYPE == 'VARCHAR2') or  (i.DATA_TYPE == 'CHAR') or  (i.DATA_TYPE == 'NVARCHAR2'):
            schemaDF = schemaDF.withColumn(str("STRCHK_")+i.COLUMN_NAME, when(length(col(i.COLUMN_NAME))  > i.ORACLE_LENGTH,lit("String is greater than defined size")).otherwise(lit("None")))
        else:
            schemaDF = schemaDF.withColumn(str("CHK_")+i.COLUMN_NAME, 
                               when(col(i.COLUMN_NAME).cast("long").isNull() & col(i.COLUMN_NAME).cast("long").isNotNull(),lit("Not a valid Number"))
                               .when(length(col(i.COLUMN_NAME)) > i.ORACLE_LENGTH ,lit("Column length Mismatch "))
                               .otherwise(lit("None")))


    for i in colTypeList:
        if i.DATA_TYPE != 'string' and i.DATA_TYPE != 'date':
            schemaDF = schemaDF.withColumn(i.COLUMN_NAME, col(i.COLUMN_NAME).cast(i.DATA_TYPE))

    schemaDF.printSchema()

    schemaDF.createOrReplaceTempView(tableName)
    column_list = build_nullable_list(tableName)
    print(schemaDF)

    emptyTblSchema = StructType(
        [
            StructField('tableName', StringType(),True),
            StructField('columnName', StringType(), True),
            StructField('dataType', StringType(), True),
            StructField('precision', StringType(), True),
            StructField('scale', StringType(), True),
            StructField('dataTypeCheck', StringType(), True),
            StructField('minValue', IntegerType(), True),
            StructField('maxValue', IntegerType(), True),
            StructField('unqCheck', StringType(), True),
            StructField('dateFormat', StringType(), True),
            StructField('primaryKeyCheck', StringType(), True),
            StructField('IndexCheck', StringType(), True),
        ]
    )

    explorer = spark.createDataFrame(sc.emptyRDD(), emptyTblSchema)

    print(explorer.collect())
    schemaDF.show()

    null_store = []
    # Iterate through all the columns and capture null counts for each column
    print('============= Checking for NOT NULL Constraints =======================')
    rule_type = 'NOT NULL Check'
    results={}
    for column_names in column_list.collect():
        query = "Select count(1) from " + tableName + " where " + column_names + " is NULL  OR " + column_names + " = 'NULL' "
        df1 = spark.sql(query).collect()
        for i in df1:
            results.update(i.asDict())
            res_in_num=results['count(1)']
            result_store=[rule_type,column_names,res_in_num]
        if res_in_num > 0:
            print (result_store)

    pk_column_list = build_pk_list() 
    pk_store = []

    pk_rule_type = 'primary key check'
    print('============= Checking for Primary Key =============================')
    results={}
    query1 = generateSql.get_unique_sql(tableName, pk_column_list,'')
    df2 = spark.sql(query1).collect()
    for i in df2:
        results.update(i.asDict())
        pk_in_num=results['COUNT']
        pk_store=[pk_rule_type,pk_column_list,pk_in_num]
        print (pk_store)

    print('============= Checking for Unique index column ===============')
    UI = build_uniq_idx_list(tableName)
    stringsDS = UI.rdd.map(lambda row: "%s" % (row.COLUMN_NAMES))
    unique_idx_rule_type = 'Unique index check'
    results3={}
    for i in stringsDS.collect():   
        query3 = generateSql.get_unique_sql(tableName, i,'')
        df3 = spark.sql(query3).collect()
        for j in df3:
            results3.update(j.asDict())
            res_in_num=results3['COUNT']
        result_store3=[unique_idx_rule_type,i,res_in_num]
           
        print (result_store3)

if __name__ == "__main__":
    # $example on:init_session$
   
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()
    
    #basic_df_example(spark)
    #schema_inference_example(spark)
    spark.sparkContext.setLogLevel("ERROR")
    tableName = sys.argv[1].strip()
    print(tableName)
    check_tbl_exist(spark, tableName)
    fileName = sys.argv[2].strip()
    print(fileName)
    oracle_data_validation(spark,tableName,fileName)
    #jdbc_dataset_example(spark)
    
    spark.stop()