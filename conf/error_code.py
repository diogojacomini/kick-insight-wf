
response = {
    'er_parameters': {
            'cod':23,
            'message': 'Error valor invalido nos parametros da funcao.'},
    'status_incompleto_func': {
        'cod': 2,
        'message': ''
    }
}

def get_error(sys):
    return response.get(sys)

