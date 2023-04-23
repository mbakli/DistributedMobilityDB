add_subdirectory("general")
add_subdirectory("catalog")
add_subdirectory("multirelation")
add_subdirectory("distributed_functions")
add_subdirectory("post_processing")
add_subdirectory("utils")
add_subdirectory("planner")
add_subdirectory("executor")

configure_file(distributed_mobilitydb.control ${CMAKE_BINARY_DIR}/distributed_mobilitydb.control)

