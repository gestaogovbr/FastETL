"""Módulo contendo as classes para limpeza de dados usando arquivos
diferenciais (patch) e controle de qualidade (qa).

Fluxo para limpeza de dados:

1. Criar diretório temporário para a dag run.
2. Extrair da base com DbToCSVOperator para gerar o arquivo CSV.
3. Retirar chaves duplicadas.
4. Utilizar limpadores: para cada um deles, gera um arquivo com sufixo
   -patch e outro com sufixo -qa.
5. Fazer merge dos patches (arquivos com sufixo -patch) e carregar no
   banco de dados.
6. Opcionalmente, carregar no banco os relatórios de controle de qualidade
   (arquivos com sufixo -qa).

Ao final, os arquivos e diretórios temporários são removidos.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List
import os
from datetime import datetime
import logging
from zipfile import ZipFile, ZIP_DEFLATED
from difflib import SequenceMatcher

import pandas as pd
from frictionless import Package, Resource, Schema, formats

from fastetl.hooks.gsheet_hook import GSheetHook

F11D_TO_PANDAS = {
    'integer': 'Int64',
}

class QALogLevel(Enum):
    CAST_FIX = 1
    CAST_NULL = 2
    DROP_LINE = 3
    ADD_COLUMN = 4

@dataclass
class DataPatch:
    """A data patch that will change some data in an original table.
    """
    source_id: str
    schema: str
    table: str
    primary_keys: List[str]
    columns: List[str] = field(default_factory=list)
    df: pd.DataFrame = None
    qa: pd.DataFrame = None

    @classmethod
    def from_file(cls,
        file_name: str,
        source_id: str,
        schema: str,
        table: str,
        primary_keys: List[str],
        file_format: str = None) -> "DataPatch":
        """Reads a data patch from a file.

        Args:
            file_name (str): Name of the patch file.
            source_id (str): Id of the data source.
            schema (str): Name of the database schema.
            table (str): Name of the database table.
            primary_keys (List[str]): List of primary keys (names).
            format (str, optional): Format of the specified file (optional).
                If not provided, will try to autodetect.

        Returns:
            DataPatch: An instance of the DataPatch.
        """
        if not file_format:
            if file_name.endswith(".zip"):
                file_format = "zip"
            elif file_name.endswith(".parq"):
                file_format = "parquet"
            else:
                raise ValueError(f"Unsupported format: {file_format}")

        if file_format == "zip":
            df = cls.read_zipped_csv(file_name)
        elif file_format == "parquet":
            df = pd.read_parquet(file_name)

        # load QA file, if available
        qa = None
        if file_name.rsplit(".", 1)[0].endswith("-patch"):
            qa_file_name = file_name.replace("-patch", "-qa")
            if os.path.exists(qa_file_name):
                if qa_file_name.endswith(".zip"):
                    qa = cls.read_zipped_csv(qa_file_name)
                elif qa_file_name.endswith(".parq"):
                    qa = pd.read_parquet(qa_file_name)

        return cls(
            source_id=source_id,
            schema=schema,
            table=table,
            primary_keys=primary_keys,
            columns=list(df.columns),
            df=df,
            qa=qa)

    @staticmethod
    def read_zipped_csv(file_name: str) -> pd.DataFrame:
        """Read a zipped csv file, possibly a data package.

        Args:
            file_name (str): Name of the zipped file to be read.

        Returns:
            pd.DataFrame: A Pandas dataframe.
        """
        with ZipFile(file_name) as archive:
            is_data_package = any(
                file.endswith('datapackage.json')
                for file in archive.namelist()
            )

        if is_data_package:
            _, descriptor_name = os.path.split(file_name)
            descriptor_name = descriptor_name.rsplit('.', 1)[0] # sem o .zip
            descriptor_name += '.datapackage.json'
            package = Package(
                file_name,
                control=formats.zip.ZipControl(innerpath=descriptor_name))
            resource = package.get_resource(package.resource_names[0])
            df = resource.to_pandas()
            # guarda as colunas de índice (chaves primárias), se houver
            if all(df.index.names):
                indexes = df.index.names
                df = df.reset_index()
            else:
                indexes = None
            # converte para os tipos corretos de colunas
            for column in resource.schema.fields:
                if column.type in F11D_TO_PANDAS:
                    df[column.name] = df[column.name].astype(
                        F11D_TO_PANDAS[column.type])
            if indexes: # se houver,
                df.set_index(indexes, inplace=True) # restaura os índices
        else:
            df = pd.read_csv(file_name, compression='zip')

        return df

    @staticmethod
    def write_zipped_csv(df: pd.DataFrame, filename: str,
                            output_path='.', index: bool=False):
        """Records the given dataframe as a zipped csv.

        Args:
            df (pd.DataFrame): The dataframe to be recorded.
            filename (str): The name of the file to be written (without
                an extension).
            output_path (str, optional): Path to the directory where the
                file will be written. Defaults to '.'.
            index (bool, optional): Wether or not to write the
                dataframe's index (same as Pandas). Defaults to False.
        """

        # entra no diretório
        os.chdir(output_path)

        # grava o esquema para o data package
        package = Package(name=filename.lower())
        resource = Resource(f'{filename}.csv', schema=Schema.describe(df))
        package.add_resource(resource)

        # inclui tudo em um pacote zip
        logging.info('Gravando "%s"...', f'{filename}.zip')
        with ZipFile(f'{filename}.zip', 'w',
                compression=ZIP_DEFLATED) as archive:
            archive.writestr(f'{filename}.datapackage.json', package.to_json())
            archive.writestr(f'{filename}.csv', df.to_csv(index=index))

    def to_file(self, output_path: str, file_format: str = "csv"):
        """Write the file that registers the changes to be made in data.
        The file has a '-patch' suffix.

        Args:
            output_path (str): Path to the directory where the file will
                be written.
            file_format (str, optional): File format. Defaults to "csv".
        """
        # verify whether is data to be written
        if self.qa.empty:
            raise ValueError("Quality control dataframe is empty, "
               " no data to write.")

        # prepara um dataframe com só o que mudou
        control = self.qa.copy() # a partir da tabela de QA
        control[self.primary_keys] = (
            control['primary_keys_values']
            .astype(str)
            .str.split(',', expand=True)
        )

        # adiciona as colunas novas
        new_columns = (
            control[control['nivel_erro'] == QALogLevel.ADD_COLUMN.value]['coluna']
            .unique()
        )
        columns = list(self.columns) + list(new_columns)
        logging.info("Colunas adicionadas: %s", str(new_columns))
        logging.info("Colunas do arquivo patch: %s", str(columns))

        os.chdir(output_path)
        df = self.df.copy()
        file_name = (
            f'{self.source_id}-{self.schema}-{self.table}-'
            f'{"-".join(columns)}-'
            'patch')

        # reindexar para gravar somente as linhas que foram alteradas
        for key in self.primary_keys:
            if (pd.api.types.infer_dtype(df[key]) == "string") and \
                all(df[key].str.isnumeric()):
                df[key] = df[key].astype("int64")
            if all(control[key].str.isnumeric()):
                control[key] = control[key].astype("int64")
        df.set_index(self.primary_keys, inplace=True)
        control.set_index(self.primary_keys, inplace=True)

        # prepara dataframe de saída
        if columns:
            # seleciona somente linhas alteradas e colunas processadas
            output = df.loc[control.index, columns].copy()
        else:
            # se não houver coluna selecionada, usa todas as colunas
            output = df.loc[control.index].copy()

        # grava o arquivo csv
        if file_format == "csv":
            self.write_zipped_csv(output, file_name,
                output_path=output_path, index=True)
        elif file_format == "parquet":
            output.to_parquet(
                os.path.join(output_path, f"{file_name}.parq"),
                index=True)

    def write_qa(self, output_path: str):
        """Writes a csv file that registers what has been and what hasn't
        been accepted in the original data, for quality assurance (QA).
        The file has a '-qa.zip' suffix.

        Args:
            output_path (str): Path to the directory where the file will
                be written.
        """
        file_name = (
            f'{self.source_id}-{self.schema}-{self.table}-'
            f'{"-".join(self.columns)}-'
            'qa')
        DataPatch.write_zipped_csv(self.qa, file_name,
            output_path=output_path, index=False)

class BaseDataCleaner(ABC):
    """Classe abstrata que define a interface para uma classe limpadora
    de dados.
    """
    def __init__(self,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str], # colunas que identificam as linhas
        columns: list[str], # colunas a serem limpas
        df: pd.DataFrame = None,
        ):

        self.df = df
        self.source_id = source_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.columns = columns
        qa_columns = [
            'primary_keys_labels', 'primary_keys_values', 'esquema', 'tabela',
            'coluna', 'valor_original', 'valor_considerado', 'nivel_erro',
            'motivo', 'datahora_verificacao']
        self.qa = pd.DataFrame(columns=qa_columns)

    def _qa_log(self, row_id: list, column: str, original_value: str,
        considered_value: str, level: int, reason: str):
        """Registra ocorrência no log de QA.

        :param row_id: Lista de valores das chaves primárias que identificam
            a linha onde ocorreu o problema.
        :type row_id: list

        :param column: Nome da coluna onde o problema foi encontrado.
        :type column: str

        :param original_value: Valor na base de origem.
        :type column: str

        :param considered_value: Valor considerado como correto. None
            caso o valor seja recusado.
        :type column: str

        :param level: Nível do erro encontrado.
            1. Aceito, dado transformado com base em regra.
            2. Recusado, dado considerado como NULL.
            3. Recusado, linha omitida da tabela.
        :type column: int

        :param reason: Motivo da não validação do(s) valor(es).
        :type reason: str
        """
        row = [
            ','.join(self.primary_keys), ','.join(map(str,row_id)),
            self.schema_name, self.table_name, column,
            original_value, considered_value,
            level.value, reason, datetime.now()]
        self.qa.loc[len(self.qa.index)] = row

    @abstractmethod
    def clean(self):
        """Realiza a tarefa de limpeza dos dados. Deve ser implementado
        por classes herdeiras.
        """

    def write(self, output_path: str, file_format: str = "csv"):
        """Grava os arquivos de patch (sufixo '-patch.zip') e de controle
        de qualidade (sufixo '-qa.zip').
        """
        if not self.qa.empty:
            patch = DataPatch(
                source_id=self.source_id,
                schema=self.schema_name,
                table=self.table_name,
                primary_keys=self.primary_keys,
                columns=self.columns,
                df=self.df,
                qa=self.qa)
            patch.to_file(output_path=output_path, file_format=file_format)
            patch.write_qa(output_path=output_path)
        else:
            logging.info('Dataframe QA vazio, nenhum arquivo a gravar.')

class OverwritingDataCleaner(BaseDataCleaner, ABC):
    """Classe abstrata para limpadores que reescrevem o mesmo arquivo.
    """
    def __init__(self,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str], # colunas que identificam as linhas
        file_name: str = None, # nome do arquivo que originou o dataframe
        df: pd.DataFrame = None,
        ):

        self.file_name = file_name
        # Get columns from dataframe if available. The patch is an
        # overwriting patch, which means all columns will be written.
        columns = list() if df is None else list(df.columns)

        super().__init__(
            source_id, schema_name, table_name, primary_keys, columns, df=df)

    def _write_patch_df(self, output_path: str):
        """Grava o arquivo csv com as duplicatas removidas,
        sobrescrevendo o original.
        """
        # sobrescreve o arquivo csv, sem as linhas duplicadas
        self.df.to_csv(
            os.path.join(output_path, self.file_name),
            compression={
                'method': 'zip',
                'archive_name': self.file_name
            },
            index=False)

class DuplicatedRowCleaner(OverwritingDataCleaner):
    """Limpador que remove linhas duplicadas."""
    def clean(self):
        "Remove as linhas duplicadas."
        df = self.df
        if df.empty:
            logging.info("Dataframe is empty, nothing to clean.")
            return

        duplicated = df.duplicated(subset=self.primary_keys, keep='first')
        if df[duplicated].empty:
            logging.info("No rows with duplicated primary keys were found in data.")
            return
        logging.info("Found %d duplicated rows.", len(df[duplicated]))

        df.loc[duplicated].apply(
            lambda row:
            self._qa_log(
                row_id=[getattr(row, key) for key in self.primary_keys],
                column=','.join(self.primary_keys),
                original_value=None,
                considered_value=None,
                level=QALogLevel.DROP_LINE,
                reason=('Ignorada linha com chave(s) primária(s) ' +
                    'duplicada(s): ' +
                    ','.join([
                        str(getattr(row, key))
                        for key in self.primary_keys]))
            ),
            axis=1)
        df.drop(index=df.loc[duplicated].index, inplace=True)

        self.df = df

class GeoPointDataCleaner(BaseDataCleaner):
    """Classe para fazer a limpeza de colunas do tipo coordenadas.

    :param df: O DataFrame pandas de entrada a ser limpo.
    :type df: class:`pd.DataFrame`

    :param primary_keys: As colunas que podem ser consideradas como
        chaves primárias, como uma lista de strings.
    :type primary_keys: list[str]

    :param columns: As duas colunas que serão consideradas como latitude
        e longitude.
    :type columns: list[str]
    """
    def __init__(self,
        df: pd.DataFrame,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str], # colunas que identificam as linhas
        columns: list[str] # colunas a serem limpas
        ):

        if len(columns) != 2:
            raise ValueError(f'Foram informadas {len(columns)} colunas. '
                'A limpeza de coordenadas requer 2 colunas.')
        self.latitude_column, self.longitude_column = columns

        super().__init__(
            source_id, schema_name, table_name, primary_keys, columns, df=df)

    def _fix_float(self, row_id: list, value, column: str):
        "Tenta corrigir valores que deveriam ser float."
        if isinstance(value, str):
            if ',' in value:
                new_value = value.replace(',', '.')
                self._qa_log(
                    row_id=row_id,
                    column=column,
                    original_value=value,
                    considered_value=new_value,
                    level=QALogLevel.CAST_FIX,
                    reason=('Vírgula como separador decimal, '
                        'substituído por ponto.')
                )
                value = new_value
            try:
                value = float(value)
            except ValueError:
                self._qa_log(
                    row_id=row_id,
                    column=column,
                    original_value=value,
                    considered_value=None,
                    level=QALogLevel.CAST_NULL,
                    reason=f'Valor "{value}" não conversível para float.')
                value = None
        return value

    def clean(self):
        "Limpa as colunas de latitude e longitude."
        if self.df.empty:
            logging.info("Dataframe is empty, nothing to clean.")
            return
        df = self.df

        # Verificações de separador de casas decimais e tipagem da coluna
        for column in (self.latitude_column, self.longitude_column):
            df[column] = df.loc[:, self.primary_keys + [column]].apply(
                lambda v:
                self._fix_float(
                    row_id=[v.qru_corrida],
                    value=getattr(v, column), # pylint: disable=cell-var-from-loop
                    column=column), # pylint: disable=cell-var-from-loop
                axis=1)

        # coordenadas com a vírgula fora do lugar
        scale_factor = 1e5
        latitude_brasil_escala_errada = (
            (
                (df[self.latitude_column] > -35.0 * scale_factor) &
                (df[self.latitude_column] < -90.0)
            ) |
            (
                (df[self.latitude_column] > 90.0) &
                (df[self.latitude_column] < 5.0 * scale_factor)
            )
        )
        logging.info('Encontradas %d linhas com latitude em escala errada.',
            len(df[latitude_brasil_escala_errada]))
        for row in df[latitude_brasil_escala_errada].to_dict('records'):
            self._qa_log(
                row_id=[row['qru_corrida']],
                column=self.latitude_column,
                original_value=str(row[self.latitude_column]),
                considered_value=str(row[self.latitude_column] / scale_factor),
                level=QALogLevel.CAST_FIX,
                reason=('Valor de latitude fora dos limites: '
                    f'{row[self.latitude_column]}. '
                    'Valor considerado: '
                    f'{row[self.latitude_column] / scale_factor}.'),
            )
        df.loc[latitude_brasil_escala_errada, self.latitude_column] = \
            df.loc[
                latitude_brasil_escala_errada,
                self.latitude_column
            ] / scale_factor

        longitude_brasil_escala_errada = (
            (df[self.longitude_column] > -76.0 * scale_factor) &
            (df[self.longitude_column] < -29.0 * scale_factor)
        )
        logging.info('Encontradas %d linhas com longitude em escala errada.',
            len(df[latitude_brasil_escala_errada]))
        for row in df[longitude_brasil_escala_errada].to_dict('records'):
            self._qa_log(
                row_id=[row['qru_corrida']],
                column=self.longitude_column,
                original_value=str(row[self.longitude_column]),
                considered_value=str(row[self.longitude_column] / scale_factor),
                level=QALogLevel.CAST_FIX,
                reason=('Valor de longitude fora dos limites: '
                    f'{row[self.longitude_column]}. '
                    'Valor considerado: '
                    f'{row[self.longitude_column] / scale_factor}.'),
            )
        df.loc[longitude_brasil_escala_errada, self.longitude_column] = \
            df.loc[
                longitude_brasil_escala_errada,
                self.longitude_column
            ] / scale_factor

        # coordenadas com valores inválidos
        latitude_inexistente = (
            (df[self.latitude_column] < -90.0) |
            (df[self.latitude_column] > 90.0)
        )
        logging.info('Encontradas %d linhas com latitude inválida.',
            len(df.loc[latitude_inexistente]))

        if not df.loc[latitude_inexistente].empty:
            df.loc[latitude_inexistente].apply(
                lambda row:
                self._qa_log(
                    row_id=[row.qru_corrida],
                    column=self.latitude_column,
                    original_value=str(getattr(row,self.latitude_column)),
                    considered_value=None,
                    level=QALogLevel.CAST_NULL,
                    reason=('Valor de latitude fora dos limites: '
                        f'{getattr(row, self.latitude_column)}.')),
                axis=1)
            df.loc[latitude_inexistente, self.latitude_column] = None

        longitude_inexistente = (
            (df[self.longitude_column] < -180.0) |
            (df[self.longitude_column] > 180.0)
        )
        logging.info('Encontradas %d linhas com longitude inválida.',
            len(df.loc[longitude_inexistente]))

        if not df.loc[longitude_inexistente].empty:
            df.loc[longitude_inexistente].apply(
                lambda row:
                self._qa_log(
                    row_id=[row.qru_corrida],
                    column=self.longitude_column,
                    original_value=str(getattr(row,self.longitude_column)),
                    considered_value=None,
                    level=QALogLevel.CAST_NULL,
                    reason=('Valor de longitude fora dos limites: '
                        f'{getattr(row, self.longitude_column)}.')),
                axis=1)
            df.loc[longitude_inexistente, self.longitude_column] = None

        self.df = df

    @staticmethod
    def sql_point(latitude: float, longitude: float, syntax='WKT') -> str:
        """Retorna a expressão SQL para o ponto."""
        if pd.isna(latitude) or pd.isna(longitude):
            return None

        if syntax=='WKT':
            expression = (
                f'POINT({longitude:.5f} '
                f'{latitude:.5f})')
        else:
            expression = (
                f'geography::Point({longitude:.5f},'
                f'{latitude:.5f},4326)')
        return expression

class CPFCleaner(BaseDataCleaner):
    """Limpa o campo CPF."""
    SEPARATORS = '.-'

    @staticmethod
    def _validate_digits(cpf: str):
        """Verifica se os dígitos verificadores são válidos.
        """
        cpf_numbers = [int(char) for char in cpf if char.isdigit()]
        for i in range(9, 11): # os dois últimos dígitos
            calculated = sum((cpf_numbers[num] * ((i+1) - num) for num in range(0, i)))
            digit = ((calculated * 10) % 11) % 10
            if digit != cpf_numbers[i]:
                return False
        return True

    def _fix_cpf(self, row_id: list, value, column: str):
        """Função de limpeza do valor do CPF a ser usada em um DataFrame
        com o método apply.
        """
        if value is None or value == '':
            return None
        if isinstance(value, float):
            value = f'{value:1.0f}'
        else:
            value = str(value)

        garbage = sum(
            1 for c in value
            if not c.isnumeric() and c not in self.SEPARATORS)
        if garbage:
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=value,
                considered_value=None,
                level=QALogLevel.CAST_NULL,
                reason=f'Valor desconsiderado por conter {garbage} '
                    f'caractere{"s" if garbage > 1 else ""} inválido'
                    f'{"s" if garbage > 1 else ""}.')
            return None

        numeric_chars = ''.join(c for c in value if c.isnumeric())[:11]

        if int(numeric_chars) < 2:
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=value,
                considered_value=None,
                level=QALogLevel.CAST_NULL,
                reason='Valor desconsiderado por estar zerado.')
            return None

        if len(set(numeric_chars)) == 1:
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=value,
                considered_value=None,
                level=QALogLevel.CAST_NULL,
                reason='Valor desconsiderado por conter 11 dígitos repetidos.')
            return None

        if len(numeric_chars) < 11:
            zero_padded = f'{int(numeric_chars):011}'
            if not self._validate_digits(zero_padded):
                self._qa_log(
                    row_id=row_id,
                    column=column,
                    original_value=value,
                    considered_value=None,
                    level=QALogLevel.CAST_NULL,
                    reason='Valor desconsiderado por conter menos de 11 dígitos.')
                return None
            self._qa_log(
                    row_id=row_id,
                    column=column,
                    original_value=value,
                    considered_value=zero_padded,
                    level=QALogLevel.CAST_FIX,
                    reason='Preenchidos zeros à esquerda por conter menos de 11 dígitos.')
            numeric_chars = zero_padded

        if not self._validate_digits(numeric_chars):
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=value,
                considered_value=None,
                level=QALogLevel.CAST_NULL,
                reason=('Valor desconsiderado pois os 2 dígitos '
                    'verificadores não conferem.')
            )

        if len(value) > len(numeric_chars):
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=value,
                considered_value=numeric_chars,
                level=QALogLevel.CAST_FIX,
                reason='Retirados separadores do valor do CPF.')

        return numeric_chars

    def clean(self):
        """Remove os caracteres não numéricos do CPF"""
        df = self.df
        if df.empty:
            return
        for column in self.columns:
            df[column] = df.apply(
                lambda row:
                self._fix_cpf(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    value=getattr(row, column), # pylint: disable=cell-var-from-loop
                    column=column), # pylint: disable=cell-var-from-loop
                axis=1)
        self.df = df

class UFCleaner(BaseDataCleaner):
    VALID_UFS = ("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
        "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ",
        "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO")

    def _discard_uf(self, row_id: list, value, column: str):
        """Corrige a sigla de UF, registrando, quando necessário, no QA."""
        if value in self.VALID_UFS:
            return value
        self._qa_log(
            row_id=row_id,
            column=column,
            original_value=value,
            considered_value=None,
            level=QALogLevel.CAST_NULL,
            reason="Valor não reconhecido como sigla de UF.")
        return None

    def clean(self):
        """Remove as UFs não reconhecidas."""
        df = self.df
        for column in self.columns:
            if not df[df[column].notna()].empty:
                df.loc[df[column].notna(), column] = df.loc[df[column].notna()][column].str.upper()
                df[column] = df.apply(
                    lambda row:
                    self._discard_uf(
                        row_id=[getattr(row, key) for key in self.primary_keys],
                        value=getattr(row, column), # pylint: disable=cell-var-from-loop
                        column=column), # pylint: disable=cell-var-from-loop
                    axis=1)

class TextCropperCleaner(BaseDataCleaner):
    """Abrevia texto em campos que ultrapassarem o limite estabelecido.
    """
    def __init__(self,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str], # colunas que identificam as linhas
        columns: list[str], # colunas a serem limpas
        sizes: list[int], # tamanhos limite das colunas
        df: pd.DataFrame = None,
        ):

        if len(columns) != len(sizes):
            raise ValueError('A quantidade de colunas deve ser igual à '
                f'quantidade de tamanhos. len(columns) == {len(columns)},'
                f' len(sizes) == {len(sizes)}')

        self.sizes = sizes

        super().__init__(
            source_id, schema_name, table_name, primary_keys, columns, df=df)

    def _fix_size(self,
        row_id: list, column: str, text: str, size: int) -> str:
        """Função de limpeza que registra as strings que forem maiores
        que o limite e as abrevia.
        """
        if len(text) > size:
            cropped_text = text[:size-3] + '...'
            self._qa_log(
                row_id=row_id,
                column=column,
                original_value=text[:size*2],
                considered_value=cropped_text,
                level=QALogLevel.CAST_FIX,
                reason=f'Valor cortado por exceder o limite de {size} '
                    f'caracteres.')
            return cropped_text
        return text

    def clean(self):
        df = self.df
        if df.empty:
            return

        for column, size in zip(self.columns, self.sizes):
            df[column] = df.apply(
                lambda row:
                self._fix_size(
                    row_id=[getattr(row, key)
                        for key in self.primary_keys],
                    column=column,
                    text=str(getattr(row, column)),
                    size=size
                ),
                axis=1
            )

        self.df = df

class GSheetMappingCleaner(BaseDataCleaner):
    """Limpa uma coluna a partir de dados de mapeamento de uma planilha.
    """
    def __init__(self,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str], # colunas que identificam as linhas
        columns: list[str], # colunas a serem limpas
        gsheet_con_id: str, # id da conexão ao Google Sheets
        spreadsheet_id: str, # id da planilha Google Sheets
        tab: str, # nome da aba da planilha
        df: pd.DataFrame = None,
        ):

        if len(columns) > 1:
            raise ValueError('Somente uma coluna pode ser tratada de '
                'cada vez. Quantidade de colunas informada: '
                f'{len(columns)}')

        self.gsheet_con_id = gsheet_con_id
        self.spreadsheet_id = spreadsheet_id
        self.tab = tab

        super().__init__(
            source_id, schema_name, table_name, primary_keys, columns, df=df)

    def _map_value(self,
        row_id: list, column: str, old_text: str, new_text: str) -> str:
        """Função de limpeza que registra os valores canônicos na
        tabela de controle de qualidade e os retorna.
        """
        if new_text != old_text:
            self._qa_log(
                    row_id=row_id,
                    column=column,
                    original_value=old_text,
                    considered_value=new_text,
                    level=QALogLevel.CAST_FIX,
                    reason=(f'Valor "{old_text}" mapeado para o valor '
                        f'canônico "{new_text}".')
            )
        return new_text

    def clean(self):
        df = self.df
        column = self.columns[0]

        # se a coluna estiver vazia, não há nada a fazer
        if df[df[column].notna()].empty:
            return

        # dados já preenchidos na planilha, aba de mapeamento
        gsheet_hook = GSheetHook(
            conn_id=self.gsheet_con_id,
            spreadsheet_id=self.spreadsheet_id)
        gsheets_frame = gsheet_hook.get_gsheet_df(sheet_name=self.tab)

        df = df.merge(gsheets_frame, on=column, how='left')

        # coloca os valores canônicos na coluna
        df[column] = df.apply(
            lambda row:
            self._map_value(
                row_id=[getattr(row, key)
                        for key in self.primary_keys],
                column=column,
                old_text=getattr(row, column),
                new_text=row.valor_canonico
            ),
            axis=1
        )

        # remove a coluna extra
        del df['valor_canonico']

        self.df = df

class OrgaosMappingCleaner(GSheetMappingCleaner):
    """Limpa colunas de órgão e unidade administrativa a partir do
    mapeamento presente no Google Sheets.

    Args:
        df (pd.DataFrame): Data frame a ser limpo.
        schema_name (str): Nome do schema no banco de dados.
        table_name (str): Nome da tabela no banco de dados.
        primary_keys (list[str]): Lista de nomes das colunas que
            identificam as linhas.
        column_orgao_nome (str): Nome da coluna que representa o nome do
            órgão.
        column_unidade_administrativa_nome (str): Nome da coluna que
            representa o nome da unidade administrativa.
        gsheet_con_id (str): Id da conexão ao Google Sheets.
        spreadsheet_id (str): Id da planilha (spreadsheet).
        tab (str): Nome da aba da planilha (sheet).
    """
    def __init__(self,
        source_id: str,
        schema_name: str,
        table_name: str,
        primary_keys: list[str],
        column_orgao_nome: str,
        column_unidade_administrativa_nome: str,
        gsheet_con_id: str,
        spreadsheet_id: str,
        tab: str,
        df: pd.DataFrame = None,
        ):

        self.gsheet_con_id = gsheet_con_id
        self.spreadsheet_id = spreadsheet_id
        self.tab = tab
        self.column_orgao_nome = column_orgao_nome
        self.column_unidade_administrativa_nome = \
            column_unidade_administrativa_nome
        columns = [column_orgao_nome, column_unidade_administrativa_nome]

        super(GSheetMappingCleaner, self).__init__( # skip GSheetMappingCleaner
            source_id, schema_name, table_name, primary_keys, columns, df=df)

    def _map_orgao(self, row: pd.Series) -> pd.Series:
        """Mapeia os valores de órgãos e unidades administrativas
        conforme os valores da tabela de mapeamento.

        Args:
            row (pd.Series): A linha da tabela, como passada pelo método
                .apply(_map_orgao, axis=1)

        Returns:
            pd.Series: A linha modificada da tabela.
        """
        present = lambda value: (not pd.isna(value)) and bool(value)
        orgao_nome_base = row.orgao_nome_base
        unidade_administrativa_nome_base = row.unidade_administrativa_nome_base
        orgao_codigo_siorg = row.orgao_codigo_siorg if (
            row.orgao_codigo_siorg and (row.orgao_codigo_siorg != -1)
        ) else None

        if not present(orgao_nome_base):
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column=",".join(self.primary_keys),
                    original_value=None,
                    considered_value=None,
                    level=QALogLevel.DROP_LINE,
                    reason=("Nome do órgão em branco na base de origem. "
                        "Linha ignorada.")
            )
            return row
        logging.info("orgao_nome_base: %s", orgao_nome_base)

        if not present(row.categoria):
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column=",".join(self.primary_keys),
                    original_value=None,
                    considered_value=None,
                    level=QALogLevel.DROP_LINE,
                    reason=(f"Orgão '{orgao_nome_base}' não mapeado na "
                        "tabela. Linha ignorada.")
            )
            return row

        self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column="orgao_categoria",
                    original_value=None,
                    considered_value=row.categoria,
                    level=QALogLevel.ADD_COLUMN,
                    reason=("Categoria do órgão ajustada para "
                        f'{row.categoria}.')
            )

        if present(orgao_codigo_siorg):
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column="orgao_codigo_siorg",
                    original_value=None,
                    considered_value=orgao_codigo_siorg,
                    level=QALogLevel.ADD_COLUMN,
                    reason=("Código do órgão ajustado para "
                        f'{orgao_codigo_siorg}.')
            )

        if orgao_nome_base != row.orgao_nome:
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column=self.column_orgao_nome,
                    original_value=orgao_nome_base,
                    considered_value=row.orgao_nome,
                    level=QALogLevel.CAST_FIX,
                    reason=(f"Nome do órgão '{orgao_nome_base}' alterado para "
                        f"valor canônico {row.orgao_nome}.")
            )
            orgao_nome_base = row.orgao_nome

        if present(row.unidade_codigo_siorg):
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column="unidade_administrativa_codigo_siorg",
                    original_value=None,
                    considered_value=row.unidade_codigo_siorg,
                    level=QALogLevel.ADD_COLUMN,
                    reason=("Código da unidade ajustado para "
                        f'{row.unidade_codigo_siorg}.')
            )

        if not present(unidade_administrativa_nome_base) and present(row.unidade_nome):
            self._qa_log(
                    row_id=[getattr(row, key) for key in self.primary_keys],
                    column=self.column_unidade_administrativa_nome,
                    original_value=unidade_administrativa_nome_base,
                    considered_value=row.unidade_nome,
                    level=QALogLevel.CAST_FIX,
                    reason=(f"Nome da unidade administrativa ajustado para "
                        f"valor canônico {row.unidade_nome}.")
            )
            unidade_administrativa_nome_base = row.unidade_nome

        setattr(row, self.column_orgao_nome, orgao_nome_base)
        row.orgao_codigo_siorg = orgao_codigo_siorg
        setattr(row,
            self.column_unidade_administrativa_nome,
            unidade_administrativa_nome_base)

        return row

    def clean(self):
        df = self.df

        # check if dataframe is empty
        if df.empty:
            logging.info("Dataframe is empty, nothing to map.")
            return

        column_orgao_nome = self.column_orgao_nome
        column_unidade_administrativa_nome = \
            self.column_unidade_administrativa_nome

        # dados já preenchidos na planilha, aba de mapeamento
        gsheet_hook = GSheetHook(
            conn_id=self.gsheet_con_id,
            spreadsheet_id=self.spreadsheet_id)
        gsheets_frame = gsheet_hook.get_gsheet_df(sheet_name=self.tab)

        df = df.rename(columns={
            column_orgao_nome: "orgao_nome_base",
            column_unidade_administrativa_nome: "unidade_administrativa_nome_base"
        })
        df[column_unidade_administrativa_nome] = None
        logging.info("Colunas no dataframe antes do merge: %s", str(df.columns))
        df = df.merge(gsheets_frame,
            on=["orgao_nome_base", "base_origem"],
            how="left")
        logging.info("Colunas no dataframe após merge: %s", str(df.columns))

        # coloca os valores canônicos na coluna
        columns = (self.primary_keys + self.columns + [
            "orgao_nome_base", "unidade_administrativa_nome_base", # vem da base de origem
            "categoria", "orgao_codigo_siorg", "unidade_codigo_siorg",
            "unidade_nome" # vem da planilha
        ])
        df.loc[:, columns] = df.loc[:, columns].apply(self._map_orgao, axis=1,)
        logging.info('Valores únicos orgao_codigo_siorg: %s', df["orgao_codigo_siorg"].unique())

        # exclui colunas originais que não são mais necessárias
        df = df.drop(columns=["orgao_nome_base", "unidade_administrativa_nome_base"])

        # renomeia colunas
        df = df.rename(columns={
            "categoria": "orgao_categoria",
            "unidade_codigo_siorg": "unidade_administrativa_codigo_siorg",
        })
        # corrige tipagem de colunas int
        for column in ("unidade_administrativa_codigo_siorg", "orgao_codigo_siorg"):
            df[column] = (
                df[column]
                .replace("", pd.NA)
                .astype("Int64")
            )
        logging.info("Colunas no dataframe no final: %s", str(df.columns))

        self.df = df

def longest_common_substring(a: str, b: str) -> str:
    "Retorna a maior substring comum entre duas strings."
    match = (
        SequenceMatcher(None, a, b)
        .find_longest_match(0, len(a), 0, len(b))
    )
    return a[match.a : match.a + match.size]

def merge_patches(tmp_dir: str, source: str, source_config: dict,
        adjust_dataframe: Callable[[pd.DataFrame], pd.DataFrame] = None) -> bool:
    """Pega todos os arquivos patchwork:

    1. arquivos com sufixo -patch, que são as diferenças registradas
       pelas tasks de limpeza;
    2. arquivos com sufixo -qa, que é são os registros de controle de
       qualidade (podendo conter também a exclusão de registros)

    e aplica essas modificações na tabela original para obter os dados
    corrigidos e gravá-los em um arquivo consolidado.

    Args:
        tmp_dir (str): Diretório temporário criado pela task
            create_tmp_dir, usado para todos os arquivos patch.
        source (str): O identificador da fonte de dados (ex.:
            TAXIGOV_DF_3).
        source_config (dict): Dicionário com as configurações da fonte,
            contendo primary_keys, schema, table e, opcionalmente,
            geo_columns.
        adjust_dataframe (Callable[[pd.DataFrame], pd.DataFrame]): função
            opcional para realizar ajustes finais no dataframe antes da
            consolidação final.


     Raises:
        ValueError: Se a quantidade de chaves informada em source_config
            diferir da quantidade de chaves encontrada nos arquivos de QA.

    Returns:
        bool: True se há dados a carregar, False caso contrário.
    """
    primary_keys = source_config['primary_keys']
    file_name = os.path.join(tmp_dir, f'{source}.zip')

    # ler o arquivo dos dados de origem
    df = DataPatch.read_zipped_csv(file_name)
    df.set_index(primary_keys, inplace=True)

    # processar os diffs (arquivos -patch) para alterações de células
    file_prefix = (f"{source}-{source_config['schema']}-"
        f"{source_config['table']}")
    patch_files = [
        os.path.join(tmp_dir, file) for file in os.listdir(tmp_dir)
        if file.startswith(f'{file_prefix}') and
            (file.endswith('-patch.zip') or file.endswith('-patch.parq'))]
    if not patch_files:
        logging.warning('Nenhum arquivo patch encontrado!')
    for patch_file in patch_files:
        logging.info('Atualizando valores a partir de %s...',
            os.path.split(patch_file)[-1])
        # lê arquivo de diffs
        if patch_file.endswith('-patch.zip'):
            df_fixes = DataPatch.read_zipped_csv(patch_file)
        elif patch_file.endswith('-patch.parq'):
            df_fixes = pd.read_parquet(patch_file)

        # registrar warning no log se ainda houver duplicatas
        df_fixes_duplicated = df_fixes.index.duplicated(keep='first')
        duplicated_quantity = sum(map(int, df_fixes_duplicated))
        if duplicated_quantity:
            logging.warning('Encontradas %d linhas duplicadas no '
                'arquivo %s.',
                duplicated_quantity,
                os.path.split(patch_file)[-1])
        df_fixes = df_fixes[~df_fixes_duplicated]
        df.loc[df_fixes.index, df_fixes.columns] = df_fixes

    # remoções de linhas
    exclude_dataframes = []
    qa_csvs = [
        os.path.join(tmp_dir, file) for file in os.listdir(tmp_dir)
        if file.startswith(f'{file_prefix}') and file.endswith('-qa.zip')]
    for qa_file_name in qa_csvs:
        # lê arquivo de log de QA
        df_qa = DataPatch.read_zipped_csv(qa_file_name)
        # linhas a remover neste arquivo de QA
        exclude_by_qa = (
            df_qa[df_qa.nivel_erro == QALogLevel.DROP_LINE.value]
            ['primary_keys_values']
            .str.split(',', expand=True)
        )
        if not exclude_by_qa.empty:
            logging.info('Removendo %d linhas a partir do registro '
                'no arquivo %s.', len(exclude_by_qa), qa_file_name)
            for column in exclude_by_qa.columns:
                exclude_by_qa[column] = exclude_by_qa[column].apply(int)
            if len(exclude_by_qa.columns) != len(primary_keys):
                raise ValueError('Quantidade de chaves primárias no '
                    f'arquivo "{qa_file_name}" difere do informado '
                    'no arquivo de configuração.')
            exclude_by_qa.columns = primary_keys
            exclude_dataframes.append(exclude_by_qa)
    if exclude_dataframes:
        keys_to_remove = (
            pd.concat(exclude_dataframes)
            .drop_duplicates()
            .set_index(list(primary_keys))
        )
        df = df.drop(keys_to_remove.index)

    if df.empty:
        logging.warning("Todos os registros foram prescindidos pelo "
            "controle de qualidade. Desistindo da carga.")
    else:
        # converte as colunas de coordenadas para tipo geography, se existirem
        if 'geo_columns' in source_config.keys():
            for geo_column in source_config['geo_columns']:
                latitude_column = geo_column['latitude']
                longitude_column = geo_column['longitude']
                df[latitude_column] = pd.to_numeric(df[latitude_column])
                df[longitude_column] = pd.to_numeric(df[longitude_column])
                coordinates_name = (
                    longest_common_substring(
                        geo_column['latitude'], geo_column['longitude']
                    ).rsplit('_', 1)[0])
                df[coordinates_name] = df.apply(
                    lambda row: GeoPointDataCleaner.sql_point(
                        getattr(row, latitude_column), # pylint: disable=cell-var-from-loop
                        getattr(row, longitude_column), # pylint: disable=cell-var-from-loop
                    ),
                    axis=1
                )
                del df[latitude_column]
                del df[longitude_column]

        # realiza ajustes finais no dataframe, se houver
        if adjust_dataframe is not None:
            df = adjust_dataframe(df)

        # troca o nome da base_origem
        df['base_origem'] = source

        logging.info('Colunas no dataframe: %s', str(df.columns))

        # grava o arquivo parquet consolidado
        df.reset_index().to_parquet(
            os.path.join(tmp_dir, f"{source}-consolidated.parq"), index=False)

    # apaga os arquivos temporários já consumidos
    for file in patch_files:
        os.remove(file)
    os.remove(file_name)

    return not df.empty
