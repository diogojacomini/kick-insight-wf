import os

dbfs_destination_path = "/FileStore/tables/insight-wf"

github_repo_url = "https://github.com/diogojacomini/kick-insight-wf"
local_repo_path = "/tmp/kick-insight-wf"

os.system(f"git clone {github_repo_url} {local_repo_path}")
os.system(f"git checkout develop")

for root, dirs, files in os.walk(local_repo_path):
    for file in files:
        local_file_path = os.path.join(root, file)
        dbfs_file_path = os.path.join(dbfs_destination_path, file)
        dbutils.fs.cp("file:" + local_file_path, dbfs_file_path, True)

os.system(f"rm -rf {local_repo_path}")
