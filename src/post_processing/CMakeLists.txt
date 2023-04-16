add_library(post_processing OBJECT
        post_processing.c)

target_include_directories(post_processing INTERFACE include/post_processing)