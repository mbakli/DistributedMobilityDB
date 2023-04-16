add_library(catalog OBJECT
        table_ops.c
        nodes.c
        )

target_include_directories(catalog INTERFACE include/catalog)

