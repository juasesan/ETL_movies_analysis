from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pendulum
import requests
import pandas as pd
import matplotlib.pyplot as plt