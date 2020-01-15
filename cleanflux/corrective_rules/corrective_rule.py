class CorrectiveRule(object):
    def __init__(self, backend_host, backend_port):
        self.backend_host = backend_host
        self.backend_port = backend_port

    @staticmethod
    def description():
        """
        :return: A short description of the rule
        """
        pass

    def check(self, query, parsed_query):
        """
        Check if a given query is permitted
        :param query:
        :param parsed_query:
        :return: result.Ok() if permitted, result.Err() if not.
        """
        pass

    def action(self, user, password, schema, query, parsed_query, more=None):
        """
        Action to perform as a proxy
        :param password:
        :param user:
        :param schema:
        :param query:
        :param parsed_query:
        :param more:
        :return: Reworked data
        """
        pass
