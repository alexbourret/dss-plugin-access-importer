import dataiku
import time
import logging
from dataiku.runnables import Runnable, ResultTable
from access_parser import AccessParser
from access_importer_common import is_ms_table, filter_string, set_dataset_as_managed, get_table_data, COLUMN_DATA


logging.basicConfig(level=logging.INFO, format='dss-plugin-access-importer %(levelname)s - %(message)s')
logger = logging.getLogger()


class AccessImporterRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        logger.info("Starting access-importer plugin v0.0.1")
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        client = dataiku.api_client()
        self.project = client.get_project(self.project_key)
        folder_id = self.config.get("model_folder_id")
        self.overwrite = self.config.get("overwrite", False)
        self.project_flow = self.project.get_flow()
        self.folder = dataiku.Folder(folder_id, project_key=self.project_key)
        self.folder_paths = self.folder.list_paths_in_partition()
        self.datasets_in_project = self.list_datasets_in_project()
        self.actions_performed = dict()
        self.macro_creates_dataset = False  # A boolean used to provide an informative message to the user when the macro creates a dataset

    def get_progress_target(self):
        return (100, 'FILES')

    def list_datasets_in_project(self):
        datasets_in_project = []
        for dataset in self.project.list_datasets():
            datasets_in_project.append(dataset.get('name'))
        return datasets_in_project

    def get_flow_zone_id(self, zone_title):
        zones = self.project_flow.list_zones()
        for zone in zones:
            if zone.name == zone_title:
                return zone.id
        return self.project_flow.create_zone(zone_title)

    def delete_existing_dataset(self, title):
        create_dataset = True
        if title in self.datasets_in_project:
            if self.overwrite:
                self.project.get_dataset(title).delete()
                self.actions_performed[title] = "replaced"
            else:
                create_dataset = False
                self.actions_performed[title] = "skipped (already exists)"
        else:
            self.actions_performed[title] = "created"
            self.macro_creates_dataset = True
        return create_dataset

    def build_result_table(self):
        result_table = ResultTable()
        result_table.add_column("actions", "Actions", "STRING")

        for action_index in range(len(self.actions_performed)):
            record = []
            record.append(
                list(self.actions_performed.keys())[action_index]
                + " has been "
                + list(self.actions_performed.values())[action_index]
            )
            result_table.add_record(record)

        if self.macro_creates_dataset:
            result_table.add_record(["Please refresh this page to see new datasets."])

        return result_table

    def run(self, progress_callback):

        def update_percent(percent, last_update_time):
            new_time = time.time()
            if (new_time - last_update_time) > 3:
                progress_callback(percent)
                return new_time
            else:
                return last_update_time

        number_of_files = len(self.folder_paths)

        update_time = time.time()
        for file_index, file_path in enumerate(self.folder_paths):
            file_name = file_path.strip('/')

            with self.folder.get_download_stream(file_path) as file_handle:
                try:
                    access_parser = AccessParser(file_handle)
                except Exception as error:
                    self.actions_performed[file_name] = "skipped (not an Access file)"
                    logger.warning("Skipped {} because of error: {}".format(file_path, error))
                    continue

            target_flow_zone_id = self.get_flow_zone_id(file_name)
            catalog = access_parser.catalog

            for table_name in catalog:
                if is_ms_table(table_name):
                    # Internal table, skip it
                    continue
                table = access_parser.parse_table(table_name)
                title = table_name

                # Ensure the file name is in the title for the dataset (prepend if missing)
                if not file_name.split(".")[0] in title:
                    title = file_name.split(".")[0] + "_" + table_name

                title = filter_string(title)

                must_create_new_dataset = self.delete_existing_dataset(title)

                if must_create_new_dataset:
                    dataset = self.project.create_dataset(
                        title,
                        'Filesystem',
                        params={
                            "connection": "filesystem_folders",
                            "path": "{}/{}".format(self.project_key, title)
                        },
                        formatType='csv',
                        formatParams={'separator': '\t', 'style': 'unix', 'compress': '', 'escapeChar': '\\'}
                    )
                    if target_flow_zone_id:
                        dataset.move_to_zone(target_flow_zone_id)
                    set_dataset_as_managed(dataset)
                    output_dataset = dataiku.Dataset(title)
                    items, schema, row_length = get_table_data(table)
                    output_dataset.write_schema(schema)
                    with output_dataset.get_writer() as writer:
                        for row_index in range(0, row_length):
                            row = []
                            for item in items:
                                row.append(item[COLUMN_DATA][row_index])
                            writer.write_tuple(row)

                percent = 100*float(file_index+1)/number_of_files
                update_time = update_percent(percent, update_time)

        result_table = self.build_result_table()

        return result_table
