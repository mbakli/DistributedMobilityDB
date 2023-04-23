set(shared_library_init.c shared_library_init.c)
add_library(general OBJECT
        ${shared_library_init.c}
        rte.c)

