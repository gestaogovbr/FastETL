""" Funções de uso comum para manipular datas e horas.
"""

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

def get_reference_date(context: dict) -> datetime:
    """ Calcula a data de referência execução da DAG.

        Caso seja uma execução de agendada, será execution_date,
        que no Airflow é a data do início do intervalo de execução da
        DAG.

        Caso seja feita ativação manual (trigger DAG), poderá ser
        passado o parâmtero reference_date no JSON de configuração.
        Nesse caso, valerá esta. O parâmetro deve ser passado no
        formato ISO (ex.: 2021-01-02T12:00):

        {
            "reference_date": "2021-01-02T12:00"
        }

        Caso seja feita a ativação manual (trigger DAG) sem passar
        esse parâmetro, será levantada uma exceção.
    """

    # trigger manual, sem especificar a variavel reference_date
    if context["dag_run"].external_trigger and \
        context["dag_run"].conf is not None and \
        "reference_date" not in context["dag_run"].conf:
        raise ValueError(
            'Para executar esta DAG manualmente é necessário incluir o'+
            'parâmetro reference_date no JSON das configurações.')

    reference_date: datetime = (
        datetime.fromisoformat(
            context["dag_run"].conf["reference_date"]
        )
    ) if context["dag_run"].conf \
        else context["execution_date"] # execução agendada da dag

    return reference_date

def get_trigger_date(context: dict) -> datetime:
    """ Calcula a data de disparo da execução da DAG.

        Caso seja uma execução de agendada, será next_execution_date,
        que no Airflow é a data esperada em que a DAG seja executada
        (é igual a execution_date + o schedule_interval).

        Caso seja feita ativação manual (trigger DAG), poderá ser
        passado o parâmtero reference_date no JSON de configuração.
        Nesse caso, valerá esta. O parâmetro deve ser passado no
        formato ISO (ex.: 2021-01-02T12:00):

        {
            "trigger_date": "2021-01-02T12:00"
        }

        Caso seja feita a ativação manual (trigger DAG) sem passar
        esse parâmetro, será considerada a next_execution_date, que
        no caso é a data em que foi realizado o trigger (data atual).
    """

    trigger_date_conf: str = (
        context["dag_run"].conf
        .get(
            "trigger_date", # trigger manual, especificando a variável
            None # ou com trigger manual, mas sem especificar variável
        )
    ) if context["dag_run"] and context["dag_run"].conf else None # execução agendada da dag

    trigger_date: datetime = \
        context["next_execution_date"] \
        if trigger_date_conf is None \
        else datetime.fromisoformat(trigger_date_conf)
    return trigger_date

def last_day_of_month(the_date: date):
    """ Retorna o último dia do mês.
    """
    # obs.: não existe timedelta(months=1), timedelta só vai até days
    return (
        the_date + relativedelta(months=+1)
    ).replace(day=1) - timedelta(days=1)

def last_day_of_last_month(the_date: date):
    """ Retorna o último dia do mês anterior.
    """
    return the_date.replace(day=1) - timedelta(days=1)

# usa a mesma lógica que get_reference_date

# apenas para compor os templates abaixo, não usar em dags
base_template_reference_date = '''
{% if dag_run.conf["reference_date"] is defined %}
    {% set the_date = macros.datetime.fromisoformat(dag_run.conf["reference_date"]) %}
{% else %}
    {% if dag_run.external_trigger %}
        {{ raise_exception_fazer_trigger_dag_somente_com_a_configuracao_reference_date }}
    {% else %}
        {% set the_date = execution_date %}
    {% endif %}
{% endif %}
'''.replace('\n', '')

# para ser usado em dags
template_reference_date = (
    base_template_reference_date +
    '{{ the_date.isoformat() }}'
).strip()

template_last_day_of_month = base_template_reference_date + '''
{% set last_day_of_month = (
    the_date + macros.dateutil.relativedelta.relativedelta(months=+1)
).replace(day=1) - macros.timedelta(days=1) %}
'''.replace('\n', '')

template_last_day_of_last_month_reference_date = base_template_reference_date + '''
{% set last_day_of_last_month_reference_date =
    the_date.replace(day=1) - macros.timedelta(days=1) %}
'''.replace('\n', '')

template_ano_mes_referencia = (
    template_last_day_of_month.strip() +
    '{{ last_day_of_month.strftime("%Y%m") }}'
)

template_ano_referencia = (
    template_last_day_of_month.strip() +
    '{{ last_day_of_month.strftime("%Y") }}'
)

template_mes_referencia = (
    template_last_day_of_month.strip() +
    '{{ last_day_of_month.strftime("%m") }}'
)

template_ano_mes_dia_referencia = (
    template_last_day_of_month.strip() +
    '{{ last_day_of_month.strftime("%Y%m%d") }}'
)

template_ano_mes_referencia_anterior = (
    template_last_day_of_last_month_reference_date.strip() +
    '{{ last_day_of_last_month_reference_date.strftime("%Y%m") }}'
)

# para ser usado em templates. Tem a mesma lógica que get_trigger_date
template_trigger_date = '''
{% if dag_run.conf is defined %}
    {% if dag_run.conf["trigger_date"] is defined %}
        {% set the_date = macros.datetime.fromisoformat(dag_run.conf["trigger_date"]) %}
    {% else %}
        {% set the_date = next_execution_date %}
    {% endif %}
{% else %}
    {% set the_date = next_execution_date %}
{% endif %}
'''.replace('\n', '')

template_last_day_of_last_month = template_trigger_date + '''
{% set last_day_of_last_month =
    the_date.replace(day=1) - macros.timedelta(days=1) %}
'''.replace('\n', '')

