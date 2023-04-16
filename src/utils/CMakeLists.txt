add_library(utils OBJECT
        helper_functions.c
        planner_utils.c)

target_include_directories(utils INTERFACE include/utils)