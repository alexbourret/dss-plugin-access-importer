COLUMN_NAME = 0
COLUMN_DATA = 1


def set_dataset_as_managed(dataset):
    dataset_definition = dataset.get_definition()
    dataset_definition["managed"] = True
    dataset.set_definition(dataset_definition)


def get_unique_names(list_of_names):
    list_unique_slugs = []
    for name in list_of_names:
        slug_name = name
        if slug_name == '':
            slug_name = 'none'
        test_string = slug_name
        index = 0
        while test_string in list_unique_slugs:
            index += 1
            test_string = slug_name + '_' + str(index)
        list_unique_slugs.append(test_string)
    return list_unique_slugs


def is_ms_table(table_name):
    return table_name.startswith("MSys") or (table_name.startswith("f_") and table_name.endswith("_Data"))


def filter_string(title):
    title = '_'.join(title.split())
    title = title.replace(')', '')
    title = title.replace('(', '')
    title = title.replace('/', '_')
    return title


def get_table_data(table):
    table_items = table.items()
    schema = []
    row_length = 0
    columns_names = []
    for table_item in table_items:
        column_name = table_item[COLUMN_NAME]
        row_length = len(table_item[COLUMN_DATA])
        columns_names.append(column_name)
    columns_names = get_unique_names(columns_names)
    for column_name in columns_names:
        schema.append(
            {
                'name': '{}'.format(column_name),
                'type': 'string'
            }
        )
    return table_items, schema, row_length
