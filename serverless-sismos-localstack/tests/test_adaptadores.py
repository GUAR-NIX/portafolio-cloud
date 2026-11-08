from botocore.exceptions import ClientError

from serverless_sismos.adaptadores_aws import crear_alertador_sns, crear_guardador_dynamodb


class TablaFalsa:
    def __init__(self, error=None):
        self.error = error
        self.item = None

    def put_item(self, **argumentos):
        if self.error:
            raise self.error
        self.item = argumentos


def error_cliente(codigo):
    return ClientError({"Error": {"Code": codigo, "Message": "x"}}, "PutItem")


def test_guardador_usa_condicion_de_idempotencia():
    tabla = TablaFalsa()
    assert crear_guardador_dynamodb(tabla)({"id_evento": "a"}) is True
    assert tabla.item["ConditionExpression"] == "attribute_not_exists(id_evento)"


def test_guardador_traduce_duplicado_a_false():
    tabla = TablaFalsa(error_cliente("ConditionalCheckFailedException"))
    assert crear_guardador_dynamodb(tabla)({"id_evento": "a"}) is False


def test_guardador_no_oculta_otros_errores():
    tabla = TablaFalsa(error_cliente("AccessDeniedException"))
    try:
        crear_guardador_dynamodb(tabla)({"id_evento": "a"})
    except ClientError as error:
        assert error.response["Error"]["Code"] == "AccessDeniedException"
    else:
        raise AssertionError("Debió propagar el error")


def test_alertador_publica_json():
    class SnsFalso:
        def publish(self, **argumentos):
            self.argumentos = argumentos

    sns = SnsFalso()
    crear_alertador_sns(sns, "arn:local")({"magnitud": 5.2, "id_evento": "x"})
    assert sns.argumentos["TopicArn"] == "arn:local"
    assert '"id_evento": "x"' in sns.argumentos["Message"]

