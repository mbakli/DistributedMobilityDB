add_subdirectory("planner")
add_subdirectory("partitioning")
add_subdirectory("general")
add_subdirectory("executor")
add_subdirectory("post_processing")
add_subdirectory("distributed_functions")

configure_file(distributed_mobilitydb.control ${CMAKE_BINARY_DIR}/distributed_mobilitydb.control)

