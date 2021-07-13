import os
import csv
from time import sleep


class DataSource:
    def __init__(self, source_path=None, file_extension='csv'):
        self.__source_path = source_path
        self.__file_extension = file_extension
        self.files_name = []
        self.files_full_path = []
        self.partitions_name = []
        self.partition_field = ""

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
            print(f'{(21 + len(self.source_path)) * "+"}')
            print(f"LISTING DIRECTORY: \'{self.source_path}\'")
            print(f'{(21 + len(self.source_path)) * "+"}')
            for entry in sp:
                print(f'{index}. {entry.name}')
                index += 1

    def get_source_files(self):
        with os.scandir(self.source_path) as it:
            for entry in it:
                if entry.name.endswith(self.file_extension):
                    self.files_name.append(entry.name)
                    self.files_full_path.append((entry.path))

    def get_partitions_names(self, field: str):
        num_files = len(self.files_name)
        self.partition_field = field
        while not num_files == 0:
            file_name_temp = self.files_full_path[num_files-1]
            with open(file_name_temp, "r", encoding='Latin1') as file:
                data = csv.DictReader(file, delimiter=';')
                for x in data:
                    if not x[field] in self.partitions_name:
                        self.partitions_name.append(x[field])
            num_files -= 1

    def split_data_in_partitions(self):
        num_files = len(self.files_name)
        check_if_partition_file_exist = 0
        while not num_files <= 0:
            file_name_temp = self.files_full_path[num_files - 1]
            with open(file_name_temp, "r", encoding='Latin1') as file:
                print(f'{(17 + len(self.files_full_path[num_files - 1]))*"+"}')
                print(f'READING FILE: {self.files_full_path[num_files - 1]}')
                print(f'{(17 + len(self.files_full_path[num_files - 1]))*"+"}')
                data = csv.DictReader(file, delimiter=';')
                num_partitions = len(self.partitions_name)
                while not num_partitions == 0:
                    if not self.partitions_name[num_partitions-1] == '':
                        partition_file_name = self.source_path + "partition_" + self.partitions_name[num_partitions-1] + ".csv"
                    else:
                        partition_file_name = self.source_path + "partition_" + "null.csv"
                    if not os.path.isfile(partition_file_name):
                        check_if_partition_file_exist = 0
                    else:
                        check_if_partition_file_exist = 1
                    with open(partition_file_name, "a", encoding='Latin1') as split_file:
                        print(f'{(31 + len(self.partitions_name[num_partitions-1]) + len(partition_file_name))*"+"}')
                        print(f'WRITING PARTITION: {self.partitions_name[num_partitions-1]} no arquivo {partition_file_name}')
                        print(f'{(31 + len(self.partitions_name[num_partitions-1]) + len(partition_file_name))*"+"}')
                        writer = csv.DictWriter(split_file, data.fieldnames, delimiter=";")
                        if check_if_partition_file_exist == 0:
                            writer.writeheader()
                        for x in data:
                            if x[self.partition_field] == self.partitions_name[num_partitions-1]:
                                writer.writerow(x)
                    file.seek(0, 0)
                    num_partitions -= 1
            num_files -= 1








if __name__ == "__main__":

    source = DataSource(source_path="data/2021/", file_extension="csv")

    source.directory_list()
    print()
    source.get_source_files()
    print()
    source.get_partitions_names(field='regiao')
    print()
    print(source.partitions_name)
    print()
    source.split_data_in_partitions()