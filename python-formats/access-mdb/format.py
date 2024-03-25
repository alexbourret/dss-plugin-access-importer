from dataiku.customformat import Formatter, FormatExtractor
from access_parser import AccessParser
from access_importer_common import get_table_data, COLUMN_DATA


class AccessFormatter(Formatter):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user for the formatter instance
        are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Formatter.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.table_name = config.get("table_name")

    def get_output_formatter(self, stream, schema):
        """
        Return a OutputFormatter for this format
        :param stream: the stream to write the formatted data to
        :param schema: the schema of the rows that will be formatted (never None)
        """
        raise NotImplementedError()

    def get_format_extractor(self, stream, schema=None):
        """
        Return a FormatExtractor for this format
        :param stream: the stream to read the formatted data from
        :param schema: the schema of the rows that will be extracted. None when the extractor is used to detect the format.
        """
        return AccessFormatExtractor(stream, schema, self.table_name)


class AccessFormatExtractor(FormatExtractor):
    """
    Reads a stream in a format to a stream of rows
    """
    row_number = 0

    def __init__(self, stream, schema, table_name):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)
        if not table_name:
            raise Exception("Please set the table name.")
        self.access_parser = AccessParser(stream)
        # catalog = self.access_parser.catalog
        table = self.access_parser.parse_table(table_name)
        if not table:
            raise Exception("Table '{}' could not be found.".format(table_name))
        self.items, self.schema, self.row_length = get_table_data(table)
        self.row_number = 0
        self.columns_names = []
        for entry in self.schema:
            self.columns_names.append(entry.get("name"))

    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return self.schema

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        if self.row_number == self.row_length:
            return None
        row = {}
        for item, column_name in zip(self.items, self.columns_names):
            row[column_name] = item[COLUMN_DATA][self.row_number]
        self.row_number += 1
        return row
