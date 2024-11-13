# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS develop;
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS stage;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS develop.demo_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS stage.defualt;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG develop TO `my-databrick-project-group`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON DATABASE develop.demo_db TO `my-databrick-project-group`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME develop.demo_db.files;

# COMMAND ----------

# MAGIC %fs ls /Volumes/develop/demo_db/files

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `external-data`

# COMMAND ----------

# MAGIC %fs ls abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/

# COMMAND ----------

# MAGIC %fs ls abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/people.json

# COMMAND ----------

spark.read.json("abfss://databricks-container@akhileshsa2.dfs.core.windows.net/sample_data/people.json").display()

# COMMAND ----------

# MAGIC %fs ls abfss://databricks-container@akhileshsa2.dfs.core.windows.net/external/

# COMMAND ----------

abfss://databricks-container@akhileshsa2.dfs.core.windows.net/external/AkhileshVarmaB_Resume.pdf

# COMMAND ----------

# MAGIC %pip install tqdm
# MAGIC %pip install opencv-python
# MAGIC %pip install pymupdf

# COMMAND ----------

import fitz
import os
import io

# COMMAND ----------

pdf_path = "abfss://databricks-container@akhileshsa2.dfs.core.windows.net/external/AkhileshVarmaB_Resume.pdf"

# COMMAND ----------

pdf_bytes = spark.read.format("binaryFile").load(pdf_path).select("content").collect()[0][0]

# COMMAND ----------

pdf_stream = io.BytesIO(pdf_bytes)

# COMMAND ----------

# Open the PDF file with PyMuPDF
pdf_document = fitz.open(stream=pdf_stream, filetype="pdf")

# COMMAND ----------

# Extract text from the first page
first_page = pdf_document.load_page(0)
text = first_page.get_text()

print(text[:30])

# COMMAND ----------

try:
    document = fitz.open(pdf_path)
    print("Document opened successfully")
    print("**************\n")
except Exception as e:
    print(e)




# COMMAND ----------

document

# COMMAND ----------

try:
    with open(pdf_path, encoding= 'latin-1') as f:
        doc_contete = f.read()
        print("Document content was successfully read")
        print("**************\n")
except Exception as e:
    print(e)



# COMMAND ----------


try:
    does_file_exist = os.path.isfile(pdf_path)
    print("DOes the files exist?", does_file_exist)
    print("**************\n")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION `external-data` TO `my-databrick-project-group`;
