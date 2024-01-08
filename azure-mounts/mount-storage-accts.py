# Databricks notebook source
path = "/mnt/azure-st-batch4"
secret = dbutils.secrets.get(scope="azuresecrets", key="databricks-secret")
# print(secret)
tenent_id = "a7bb685b-b95e-4fad-bb3a-095b0e8b496a"
client_id = "fb157227-eebe-4fae-bbd9-dd0e31aacf81"
st_acct_name = "stbatch4training"
container_name = "batch4-blob"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{st_acct_name}.dfs.core.windows.net/",
  mount_point = path,
  extra_configs = configs)

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/mnt/azure-st-batch4/export.csv")
df.display()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/azure-st-batch4")
