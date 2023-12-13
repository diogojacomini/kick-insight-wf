CONNECTION_STRING = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=worldfootballrepo;AccountKey=P0LCNpIcHOfYGSKi4TVzv+3pfSmevdE9PVaAzaJi/p0cZJZtgSWSPHrcVRVe0chKB1y3aLl2VxW/+ASt1i9YsQ==;BlobEndpoint=https://worldfootballrepo.blob.core.windows.net/;FileEndpoint=https://worldfootballrepo.file.core.windows.net/;QueueEndpoint=https://worldfootballrepo.queue.core.windows.net/;TableEndpoint=https://worldfootballrepo.table.core.windows.net/"

variables = {
    'SISTEMA': [
        'campeonato-brasileiro'
        ],
    'EXEC': 'ml, pr'
}

def get_vars(name):
    return variables.get(name)
