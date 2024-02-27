from airflow.decorators import dag, task
#from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
import requests
import pandas as pd
import matplotlib.pyplot as plt