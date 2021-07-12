import os
import csv


class DataSource:
    def __init__(self, source_path=None, file_extension='csv'):
        self.__source_path = source_path
        self.__file_extension = file_extension
        self.files_name = []
        self.partitions_name = []

    @property
    def source_path(self):
        return self.__source_path

    @source_path.setter
    def source_path(self, value):
        self.__source_path = value

    @property
    def file_extension(self):
        return self.__file_extension

    @file_extension.setter
    def file_extension(self, value):
        self.__file_extension = value

    def directory_list(self):
        with os.scandir(self.source_path) as sp:
            index = 1
            for entry in sp:
                print(f'{index}. {entry.name}')
                index += 1

    def get_source_files(self):
        with os.scandir(self.source_path) as it:
            for entry in it:
                if entry.name.endswith(self.file_extension):
                    self.files_name.append(entry.name)

    def get_partitions_names(self, field: str):
        while not self.files_name == []:
            with open((self.source_path + self.files_name).pop(), "r", encoding='Latin1') as file:
                data = csv.DictReader(file, delimiter=';')
                for x in data:
                    if not x[field] in self.partitions_name and x[field] != '':
                        self.partitions_name.append(x[field])

source = DataSource(file_extension="csv")

source.directory_list()
print()
source.get_source_files()

source.get_partitions_names(field='regiao')
print(source.partitions_name)
