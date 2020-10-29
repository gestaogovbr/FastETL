"""
Doc

guilty: Vitor.
"""

# TODO
# [ ] rodar a query
#   [ ] arrumar as querys para não ficar invertida
# [ ] se query der errado, montar o print, raise exception (olhar como faz o checkoperator)
# [ ] se query der certo, pass
# [ ] Mudar operador nas DAGs

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CheckFromToOperator(BaseOperator):
    """
    Doc
    """

    ui_color = '#f502e9'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self, sql, conn_id=None, *args, **kwargs):
        super(CheckFromToOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context=None):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)

        self.log.info("Record: %s", records)
        if not records:
            self.print_guidance()
            raise AirflowException("The query returned None")
        elif not all([bool(r) for r in records]):
            raise AirflowException(
                "Test failed.\nQuery:\n{query}\nResults:\n{records!s}".format(
                    query=self.sql, records=records
                )
            )

        self.log.info("Success.")

    def get_db_hook(self):
        """
        Get the database hook for the connection.
        :return: the database hook object.
        :rtype: DbApiHook
        """
        return BaseHook.get_hook(conn_id=self.conn_id)

    def print_guidance(self):
        self.log.info("Executing SQL check: %s", self.sql)
        pass