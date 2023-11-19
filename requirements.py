import subprocess

# Caminho para o arquivo requirements.txt no DBFS
requirements_path = "/FileStore/tables/insight-wf/requirements.txt"

# Comando para instalar as dependências usando pip
pip_install_command = f"pip install -r {requirements_path}"

# Executar o comando usando subprocess
subprocess.run(pip_install_command, shell=True)
