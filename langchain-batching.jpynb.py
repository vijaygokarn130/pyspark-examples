# Databricks notebook source
# MAGIC %pip install langchain_openai
# MAGIC %pip install --upgrade langchain_core langchain_openai

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json
import time
import os
import getpass
import pandas as pd
from datasets import Dataset, load_dataset
from tqdm import tqdm
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

# COMMAND ----------

dataset = load_dataset("wikimedia/wikipedia", "20231101.en")
NUM_SAMPLES = 10000
articles = dataset["train"][:NUM_SAMPLES]["text"]
ids = dataset["train"][:NUM_SAMPLES]["id"]
articles = [x.split("\n")[0] for x in articles]

# COMMAND ----------

len(articles)

# COMMAND ----------

articles[99]


# COMMAND ----------

# MAGIC %md
# MAGIC Enter you OPEN AI key

# COMMAND ----------

os.environ["OPENAI_API_KEY"] = getpass.getpass("Enter your OpenAI API key: ")

# COMMAND ----------

llm = ChatOpenAI()
llm.model_name

# COMMAND ----------

prompt = ChatPromptTemplate.from_messages([
    ("system", """Your task is to assess the article and categorize the article into one of the following predfined categories:
'History', 'Geography', 'Science', 'Technology', 'Mathematics', 'Literature', 'Art', 'Music', 'Film', 'Television', 'Sports', 'Politics', 'Philosophy', 'Religion', 'Sociology', 'Psychology', 'Economics', 'Business', 'Medicine', 'Biology', 'Chemistry', 'Physics', 'Astronomy', 'Environmental Science', 'Engineering', 'Computer Science', 'Linguistics', 'Anthropology', 'Archaeology', 'Education', 'Law', 'Military', 'Architecture', 'Fashion', 'Cuisine', 'Travel', 'Mythology', 'Folklore', 'Biography', 'Mythology', 'Social Issues', 'Human Rights', 'Technology Ethics', 'Climate Change', 'Conservation', 'Urban Studies', 'Demographics', 'Journalism', 'Cryptocurrency', 'Artificial Intelligence'
you will output a json object containing the following information:

{{
    "id": string
    "category": string
}}
"""),
    ("human", "{input}")
])

# COMMAND ----------

chain = prompt | llm


# COMMAND ----------

content = json.dumps({"id": ids[0], "article": articles[0]})
response = chain.invoke(content)
response.content

# COMMAND ----------

batches = []
for index in range(3):
    batches.append(json.dumps({"id": ids[index], "article": articles[index]}))
chain.batch(batches)

# COMMAND ----------

json.loads(response.content)


# COMMAND ----------

# MAGIC %md
# MAGIC RUN INTERFACE

# COMMAND ----------

results = []
for article in tqdm(articles[:100]):
    try:
        result = chain.invoke({"input": article})
        results.append(result)
    except Exception as e:
        print("Exception Occured", e)
        results.append("")

# COMMAND ----------

results = []
BATCH_SIZE = 8
inputs = []

for index, article in tqdm(enumerate(articles[:1000])):
    inputs.append(json.dumps({"id": ids[index], "article": articles[index]}))
    
    if len(inputs) == BATCH_SIZE:
        time.sleep(1.5)
        response = chain.batch(inputs)
        results += response
        inputs = []
        
if inputs:
    response = chain.batch(inputs)
    results += response
           

# COMMAND ----------

pd.DataFrame([x.response_metadata["token_usage"] for x in results])


# COMMAND ----------

success = []
failure = []

for output in results:
    content = output.content
    try:
        content = json.loads(content)
        success.append(content)
    except ValueError as e:
        failure.append(content)


# COMMAND ----------

# MAGIC %md
# MAGIC Print the classifications

# COMMAND ----------

pd.DataFrame(success)

