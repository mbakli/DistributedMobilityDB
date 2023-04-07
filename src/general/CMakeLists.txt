set(shared_library_init.c shared_library_init.c)
add_library(general OBJECT
        ${shared_library_init.c}
        catalog_management.c
        helper_functions.c)

