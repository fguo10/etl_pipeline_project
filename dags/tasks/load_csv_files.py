import pandas as pd
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(current_dir, '..', '..', 'data')


def load_csv_data(file_path: str, sep=",\t") -> pd.DataFrame:
    return pd.read_csv(file_path, sep=sep, engine="python")


def read_users_file():
    users_path = data_file_path + "/users.csv"
    return load_csv_data(users_path)


def read_experiments_file():
    experiments_path = data_file_path + "/user_experiments.csv"
    return load_csv_data(experiments_path)


def read_compounds_file():
    compounds_path = data_file_path + "/compounds.csv"
    return load_csv_data(compounds_path)
