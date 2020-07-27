import pandas as pd
import subprocess
import dvc.api

def save_pandas_df_to_dvc_as_parquet_file(df, file_name, folder):
    file_path = folder + file_name
    df.to_parquet(file_path, compression='gzip')
    save_file_to_dvc(file_name, folder)

def save_file_to_dvc(file_name, folder):
    subprocess.run("dvc add " + file_name, cwd=folder)
    subprocess.run("git add {}.dvc .gitignore".format(file_name), cwd=folder)
    subprocess.run("git commit -m \"Automatic commit adding file {}\"".format(file_name), cwd=folder)
    subprocess.run("git push", cwd=folder)
    subprocess.run("dvc push", cwd=folder)

def get_pandas_df_from_dvc(file_name, repo_path):
    resource_url = dvc.api.get_url(
        file_name,
        repo=repo_path)
    df = pd.read_parquet(resource_url)
    return df

if __name__ == "__main__":
    d = {'col1': [1, 2], 'col2': [3, 999]}
    df = pd.DataFrame(data=d)
    file_name = "df_test6.parquet.gzip" # This can be decided freely
    folder = "C:\\Users\\teemu.leivo\\dvc_test\\" # The folder needs to have both git and dvc initialized. And the dvc repository needs to be the one defined in dvc_repo
    save_pandas_df_to_dvc_as_parquet_file(df, file_name, folder)
    
    dvc_repo = "https://github.com/TeemuLeivo/dvc_test"
    result_df = get_pandas_df_from_dvc(file_name, dvc_repo)
    print(result_df)
    
    
