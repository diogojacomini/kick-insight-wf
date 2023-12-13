common_params = {
    "container_name": "exemplo",
    "features": ["feature1", "feature2", "feature3"],
    "target": "target",
    "ml_params": {
        "n_estimators": 100,
        "max_depth": None,
        "min_samples_split": 2,
        "min_samples_leaf": 1,
        "random_state": 42,
    },
    "grid_search": False,
    "split_size": 0.3,
    "output": "ml/ml_exemplo_year.joblib",
}

# Parâmetros específicos para cada sistema
sistema_params = {
    "campeonato-brasileiro": {
        "container_name": "campeonato-brasileiro", 
        "features": ['vitoria', 'empate', 'derrota', 'gols', 'gols_sofridos', 'diferenca_gols', 'pontos'],
        "target": "posicao",
        "ml_params": {'max_depth': None, 'min_samples_leaf': 4, 'min_samples_split': 2, 'n_estimators': 300},
        "grid_search": False,
        "split_size": 0.3,
        "output": "ml/ml_campeonato_brasileiro_year.joblib",
    },
}

def get_params(sistema):
    return sistema_params.get(sistema)
